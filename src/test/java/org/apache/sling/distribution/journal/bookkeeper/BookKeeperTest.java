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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.function.Consumer;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.ImportPostProcessor;
import org.apache.sling.distribution.InvalidationProcessor;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.messages.LogMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Matchers.any;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.service.event.EventAdmin;

@RunWith(MockitoJUnitRunner.class)
public class BookKeeperTest {

    private static final int COMMIT_AFTER_NUM_SKIPPED = 10;

    private static final String PUB_AGENT_NAME = "pubAgent";

    private ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();

    @Mock
    private DistributionMetricsService distributionMetricsService;

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
    private ImportPostProcessor importPostProcessor;

    @Mock
    private InvalidationProcessor invalidationProcessor;

    @Before
    public void before() {
        when(distributionMetricsService.getFailedPackageImports())
                .thenReturn(mock(Meter.class));
        when(distributionMetricsService.getImportedPackageDuration())
                .thenReturn(mock(Timer.class));
        when(distributionMetricsService.getImportedPackageSize())
                .thenReturn(mock(Histogram.class));
        when(distributionMetricsService.getPackageDistributedDuration())
                .thenReturn(mock(Timer.class));
        when(distributionMetricsService.getImportPostProcessRequest())
                .thenReturn(mock(Counter.class));
        when(distributionMetricsService.getImportPostProcessDuration())
                .thenReturn(mock(Timer.class));
        when(distributionMetricsService.getImportPostProcessSuccess())
                .thenReturn(mock(Counter.class));
        when(distributionMetricsService.getInvalidationProcessRequest())
                .thenReturn(mock(Counter.class));
        when(distributionMetricsService.getInvalidationProcessDuration())
                .thenReturn(mock(Timer.class));
        when(distributionMetricsService.getInvalidationProcessSuccess())
                .thenReturn(mock(Counter.class));
        when(distributionMetricsService.getTransientImportErrors())
                .thenReturn(mock(Counter.class));
        when(distributionMetricsService.getPermanentImportErrors())
                .thenReturn(mock(Counter.class));
        when(distributionMetricsService.getPackageStatusCounter(any(String.class)))
                .thenReturn(mock(Counter.class));

        BookKeeperConfig bkConfig = new BookKeeperConfig("subAgentName", "subSlingId", true, 10, PackageHandling.Extract, "package", true);
        bookKeeper = new BookKeeper(resolverFactory, distributionMetricsService, packageHandler, eventAdmin, sender, logSender, bkConfig,
            importPostProcessor, invalidationProcessor);
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
