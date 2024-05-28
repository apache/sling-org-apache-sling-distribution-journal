/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.distribution.journal.bookkeeper;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Gauge;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.commons.metrics.internal.MetricsServiceImpl;
import org.apache.sling.distribution.ImportPostProcessor;
import org.apache.sling.distribution.ImportPreProcessor;
import org.apache.sling.distribution.InvalidationProcessor;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.messages.LogMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.matchers.GreaterOrEqual;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.osgi.service.event.EventAdmin;

import javassist.bytecode.analysis.Analyzer;

@RunWith(MockitoJUnitRunner.class)
public class BookKeeperTest {

    private static final int COMMIT_AFTER_NUM_SKIPPED = 10;

    private static final String PUB_AGENT_NAME = "pubAgent";

    private ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();

    private SubscriberMetrics subscriberMetrics;
    
    private OsgiContext context = new OsgiContext();

    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private Consumer<PackageStatusMessage> sender;

    private BookKeeper bookKeeper;

    @Mock
    private PackageHandler packageHandler;

    @Mock
    private DistributionPackageBuilder packageBuilder;

    @Mock
    private Consumer<LogMessage> logSender;

    @Mock
    private ImportPreProcessor importPreProcessor;
    
    @Mock
    private ImportPostProcessor importPostProcessor;

    @Mock
    private InvalidationProcessor invalidationProcessor;

    private MetricsService metricsService;

    @Before
    public void before() {
        metricsService = context.registerInjectActivateService(MetricsServiceImpl.class);
        BookKeeperConfig bkConfig = new BookKeeperConfig("subAgentName", "subSlingId", true, 10, PackageHandling.Extract, "package", true);
        subscriberMetrics = new SubscriberMetrics(metricsService, bkConfig.getSubAgentName(), "publish", bkConfig.isEditable());
        bookKeeper = new BookKeeper(resolverFactory, subscriberMetrics, packageHandler, eventAdmin, sender, logSender, bkConfig,
                importPreProcessor, importPostProcessor, invalidationProcessor);
    }

    @Test
    public void testOnlyEveryTenthSkippedPackageOffsetStored() throws InterruptedException, PersistenceException, LoginException {
        for (int c = 0; c < COMMIT_AFTER_NUM_SKIPPED; c++) {
            bookKeeper.skipPackage(c);
            assertThat(bookKeeper.loadOffset(), equalTo(-1L));
        }
        for (int c = COMMIT_AFTER_NUM_SKIPPED; c < COMMIT_AFTER_NUM_SKIPPED * 2; c++) {
            bookKeeper.skipPackage(c);
            assertThat(bookKeeper.loadOffset(), equalTo(10L));
        }
        for (int c = COMMIT_AFTER_NUM_SKIPPED * 2; c < COMMIT_AFTER_NUM_SKIPPED * 3; c++) {
            bookKeeper.skipPackage(c);
            assertThat(bookKeeper.loadOffset(), equalTo(20L));
        }
    }

    @Test
    public void testPackageImport() throws DistributionException {
        try {
            bookKeeper.importPackage(buildPackageMessage(PackageMessage.ReqType.ADD), 10, currentTimeMillis());
        } finally {
            assertThat(bookKeeper.getRetries(PUB_AGENT_NAME), equalTo(0));
        }
    }
    
    @Test
    public void testPackageImportCurrentDuration() throws DistributionException, PersistenceException {
        assertThat(subscriberMetrics.getCurrentImportDurationCallback().get(), equalTo(0L));
        doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(500);
                Long duration = subscriberMetrics.getCurrentImportDurationCallback().get();
                if (duration < 400L) {
                    throw new IllegalStateException("Should get valid duration");
                }
                return null;
            }
            
        }).when(packageHandler).apply(Mockito.any(ResourceResolver.class), Mockito.any(PackageMessage.class));
        
        bookKeeper.importPackage(buildPackageMessage(PackageMessage.ReqType.ADD), 10, currentTimeMillis());
        
        assertThat(subscriberMetrics.getCurrentImportDurationCallback().get(), equalTo(0L));
    }

    @Test
    public void testCacheInvalidation() throws DistributionException {
        try {
            bookKeeper.invalidateCache(buildPackageMessage(PackageMessage.ReqType.INVALIDATE), 10);
        } finally {
            assertThat(bookKeeper.getRetries(PUB_AGENT_NAME), equalTo(0));
        }
    }

    PackageMessage buildPackageMessage(PackageMessage.ReqType reqType) {
        PackageMessage msg = mock(PackageMessage.class);
        when(msg.getPkgLength())
                .thenReturn(100L);
        when(msg.getPubAgentName())
                .thenReturn(PUB_AGENT_NAME);
        when(msg.getReqType())
                .thenReturn(reqType);
        when(msg.getPaths())
                .thenReturn(singletonList("/content"));
        when(msg.getPkgId())
                .thenReturn(UUID.randomUUID().toString());
        return msg;
    }

}
