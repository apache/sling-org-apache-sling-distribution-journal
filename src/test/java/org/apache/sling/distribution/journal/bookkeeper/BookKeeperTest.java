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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.MetricsService;
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
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.osgi.service.event.EventAdmin;

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
        BookKeeperConfig bkConfig = new BookKeeperConfig("subAgentName", "subSlingId", true, 10, PackageHandling.Extract, "package", "command", true);
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
            bookKeeper.importPackage(buildPackageMessage(PackageMessage.ReqType.ADD), 10, currentTimeMillis(), currentTimeMillis());
        } finally {
            assertThat(bookKeeper.getRetries(PUB_AGENT_NAME), equalTo(0));
        }
    }
    
    @Test
    public void testPackageBlockingImportErrorMetric() throws DistributionException, PersistenceException {
        doThrow(IllegalStateException.class) 
            .when(packageHandler).apply(Mockito.any(ResourceResolver.class), Mockito.any(PackageMessage.class));
        Counter counter = subscriberMetrics.getBlockingImportErrors();
        assertThat(counter.getCount(), equalTo(0L));
        
        for (int c=0; c< BookKeeper.NUM_ERRORS_BLOCKING + 1; c++) {
            try {
                bookKeeper.importPackage(buildPackageMessage(PackageMessage.ReqType.ADD), 10, currentTimeMillis(), currentTimeMillis());
            } catch (DistributionException e) {
            }
        }
        
        assertThat(counter.getCount(), equalTo(1L));
    }
    
    @Test
    public void testPackageImportFailCurrentDuration() throws DistributionException, PersistenceException {
        assertThat(subscriberMetrics.getCurrentImportDuration(), equalTo(0L));
        doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                long duration = subscriberMetrics.getCurrentImportDuration();
                if (duration < Duration.ofMinutes(6).toMillis()) {
                    throw new IllegalStateException("Should get valid duration");
                }
                return null;
            }
            
        }).when(packageHandler).apply(Mockito.any(ResourceResolver.class), Mockito.any(PackageMessage.class));
        
        long simulatedStartTime = currentTimeMillis() - Duration.ofMinutes(6).toMillis();
        bookKeeper.importPackage(buildPackageMessage(PackageMessage.ReqType.ADD), 10, simulatedStartTime, simulatedStartTime);
        
        assertThat(subscriberMetrics.getCurrentImportDuration(), equalTo(0L));
    }
    
    @Test
    public void testPackageImportCurrentDuration() throws DistributionException, PersistenceException {
        assertThat(subscriberMetrics.getCurrentImportDuration(), equalTo(0L));
        doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Long duration = subscriberMetrics.getCurrentImportDuration();
                if (duration < Duration.ofMinutes(1).toMillis()) {
                    throw new IllegalStateException("Should get valid duration");
                }
                return null;
            }
            
        }).when(packageHandler).apply(Mockito.any(ResourceResolver.class), Mockito.any(PackageMessage.class));
        
        long simulatedStartTime = currentTimeMillis() - Duration.ofMinutes(1).toMillis();
        bookKeeper.importPackage(buildPackageMessage(PackageMessage.ReqType.ADD), 10, currentTimeMillis(), simulatedStartTime);
        
        assertThat(subscriberMetrics.getCurrentImportDuration(), equalTo(0L));
    }

    @Test
    public void testCacheInvalidation() throws DistributionException {
        try {
            long simulatedStartTime = currentTimeMillis() - Duration.ofMinutes(1).toMillis();
            bookKeeper.invalidateCache(buildPackageMessage(PackageMessage.ReqType.INVALIDATE), 10, simulatedStartTime);
        } finally {
            assertThat(bookKeeper.getRetries(PUB_AGENT_NAME), equalTo(0));
        }
    }
    
    @Test
    public void testClearOffsetHandling() throws DistributionException {
    	Long offset = bookKeeper.getClearOffset();
    	assertThat("Should be null", offset, nullValue());
    	long newOffset = 1000;
    	bookKeeper.storeClearOffset(newOffset);
    	Long offset2 = bookKeeper.getClearOffset();
    	assertThat("Should be null", offset2, equalTo(newOffset));
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
