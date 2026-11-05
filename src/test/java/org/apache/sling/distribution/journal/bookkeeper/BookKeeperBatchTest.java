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

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.internal.MetricsServiceImpl;
import org.apache.sling.distribution.ImportPostProcessor;
import org.apache.sling.distribution.ImportPreProcessor;
import org.apache.sling.distribution.InvalidationProcessor;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.messages.LogMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BookKeeperBatchTest {

    private static final String PUB_AGENT_NAME = "pubAgent";

    private final ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();

    private final OsgiContext context = new OsgiContext();

    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private Consumer<PackageStatusMessage> sender;

    @Mock
    private Consumer<LogMessage> logSender;

    @Mock
    private PackageHandler packageHandler;

    @Mock
    private ImportPreProcessor importPreProcessor;

    @Mock
    private ImportPostProcessor importPostProcessor;

    @Mock
    private InvalidationProcessor invalidationProcessor;

    @Test
    public void testImportPackagesStoresLastOffsetOnce() throws Exception {
        BookKeeper bookKeeper = createBookKeeper(false);
        List<BatchEntry> entries = Arrays.asList(entry("a", 10), entry("b", 11), entry("c", 12));

        bookKeeper.importPackages(entries, new Date());

        verify(packageHandler)
                .applyBatch(
                        any(ResourceResolver.class),
                        eq(Arrays.asList(
                                entries.get(0).getMessage(),
                                entries.get(1).getMessage(),
                                entries.get(2).getMessage())));
        verify(importPreProcessor, times(3)).process(any());
        verify(importPostProcessor, times(3)).process(any());
        verify(eventAdmin, times(3)).postEvent(any(Event.class));
        verifyNoInteractions(sender);
        assertThat(bookKeeper.loadOffset(), equalTo(12L));
    }

    @Test
    public void testFailedBatchStoresNoOffsetAndDoesNotCountRetries() throws Exception {
        BookKeeper bookKeeper = createBookKeeper(false);
        doThrow(new DistributionException("install failed"))
                .when(packageHandler)
                .applyBatch(any(ResourceResolver.class), any());
        List<BatchEntry> entries = Arrays.asList(entry("a", 10), entry("b", 11));
        Date importStartTime = new Date();

        assertThrows(DistributionException.class, () -> bookKeeper.importPackages(entries, importStartTime));

        assertThat(bookKeeper.loadOffset(), equalTo(-1L));
        assertThat(bookKeeper.getRetries(PUB_AGENT_NAME), equalTo(0));
        verify(importPostProcessor, never()).process(any());
        verify(eventAdmin, never()).postEvent(any(Event.class));
        verifyNoInteractions(logSender);
    }

    @Test
    public void testEditableSubscriberStoresOneStatusForBatch() throws Exception {
        BookKeeper bookKeeper = createBookKeeper(true);
        List<BatchEntry> entries = Arrays.asList(entry("a", 10), entry("b", 11), entry("c", 12));

        bookKeeper.importPackages(entries, new Date());

        assertThat(bookKeeper.loadOffset(), equalTo(12L));
        LocalStore statusStore = new LocalStore(resolverFactory, BookKeeper.STORE_TYPE_STATUS, "subAgentName");
        BookKeeper.PackageStatus status = new BookKeeper.PackageStatus(statusStore.load());
        assertThat(status.status, equalTo(PackageStatusMessage.Status.IMPORTED));
        assertThat(statusStore.load().get(BookKeeper.KEY_OFFSET, Long.class), equalTo(12L));
        verifyNoInteractions(sender);

        assertThat(bookKeeper.sendStoredStatus(0), equalTo(true));
        verify(sender, times(1)).accept(any(PackageStatusMessage.class));
    }

    @Test
    public void testSupportsBatchImportFollowsPackageHandler() {
        when(packageHandler.supportsBatch()).thenReturn(true);

        assertThat(createBookKeeper(false).supportsBatchImport(), equalTo(true));
    }

    private BookKeeper createBookKeeper(boolean editable) {
        MetricsService metricsService = context.registerInjectActivateService(MetricsServiceImpl.class);
        BookKeeperConfig bkConfig = new BookKeeperConfig(
                "subAgentName", "subSlingId", editable, 10, PackageHandling.Off, "package", "command", true);
        SubscriberMetrics subscriberMetrics =
                new SubscriberMetrics(metricsService, bkConfig.getSubAgentName(), "publish", bkConfig.isEditable());
        return new BookKeeper(
                resolverFactory,
                subscriberMetrics,
                packageHandler,
                eventAdmin,
                sender,
                logSender,
                bkConfig,
                importPreProcessor,
                importPostProcessor,
                invalidationProcessor);
    }

    private static BatchEntry entry(String pkgId, long offset) {
        PackageMessage message = PackageMessage.builder()
                .pkgId(pkgId)
                .pubSlingId("pubSlingId")
                .pubAgentName(PUB_AGENT_NAME)
                .reqType(PackageMessage.ReqType.ADD)
                .pkgType("journal")
                .paths(singletonList("/content/" + pkgId))
                .pkgBinary(new byte[10])
                .build();
        return new BatchEntry(message, offset, Instant.now());
    }
}
