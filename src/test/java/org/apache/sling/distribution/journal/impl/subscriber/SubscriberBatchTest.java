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
package org.apache.sling.distribution.journal.impl.subscriber;

import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.distribution.ImportPostProcessor;
import org.apache.sling.distribution.ImportPreProcessor;
import org.apache.sling.distribution.InvalidationProcessor;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeper;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperFactory;
import org.apache.sling.distribution.journal.bookkeeper.LocalStore;
import org.apache.sling.distribution.journal.impl.precondition.Precondition;
import org.apache.sling.distribution.journal.impl.precondition.Precondition.Decision;
import org.apache.sling.distribution.journal.messages.ClearCommand;
import org.apache.sling.distribution.journal.messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.messages.PingMessage;
import org.apache.sling.distribution.journal.shared.NoOpImportPostProcessor;
import org.apache.sling.distribution.journal.shared.NoOpImportPreProcessor;
import org.apache.sling.distribution.journal.shared.NoOpInvalidationProcessor;
import org.apache.sling.distribution.journal.shared.OnlyOnLeader;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.journal.spi.BatchDistributionPackageBuilder;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.settings.SlingSettingsService;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.service.event.EventAdmin;
import org.osgi.util.converter.Converters;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SubscriberBatchTest {

    private static final String SUB1_SLING_ID = "sub1sling";
    private static final String SUB1_AGENT_NAME = "sub1agent";
    private static final String PUB1_AGENT_NAME = "pub1agent";
    private static final String STORE_PACKAGE_NODE_NAME = "myserver.apache.org_somepath_package";

    @Mock
    private BundleContext context;

    @Mock
    private BatchDistributionPackageBuilder packageBuilder;

    @Mock
    private Precondition precondition;

    @Mock
    private SlingSettingsService slingSettings;

    @Spy
    private ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();

    @Mock
    private MessagingProvider clientProvider;

    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private MessageSender<DiscoveryMessage> discoverySender;

    @Mock
    private MessageSender<PackageStatusMessage> statusSender;

    @Spy
    private MetricsService metricsService = MetricsService.NOOP;

    @Spy
    private ImportPreProcessor importPreProcessor = new NoOpImportPreProcessor();

    @Spy
    private ImportPostProcessor importPostProcessor = new NoOpImportPostProcessor();

    @Spy
    private InvalidationProcessor invalidationProcessor = new NoOpInvalidationProcessor();

    @Spy
    private SubscriberReadyStore subscriberReadyStore = new SubscriberReadyStore();

    @InjectMocks
    private BookKeeperFactory bookKeeperFactory;

    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageMessage>> packageCaptor;

    @Captor
    private ArgumentCaptor<HandlerAdapter<PingMessage>> pingCaptor;

    @Captor
    private ArgumentCaptor<HandlerAdapter<ClearCommand>> commandCaptor;

    @Captor
    private ArgumentCaptor<PackageStatusMessage> statusCaptor;

    private MessageHandler<ClearCommand> commandHandler;

    @Mock
    private Closeable poller;

    @Mock
    private Appender<ILoggingEvent> logAppender;

    private DistributionSubscriber subscriber;

    private MessageHandler<PackageMessage> packageHandler;

    private final List<List<String>> installedBatches = Collections.synchronizedList(new ArrayList<>());

    @Before
    public void before() throws Exception {
        ((Logger) LoggerFactory.getLogger(DistributionSubscriber.class)).addAppender(logAppender);
        DistributionSubscriber.QUEUE_FETCH_DELAY_MILLIS = 100;
        DistributionSubscriber.RETRY_DELAY_MILLIS = 100;
        when(packageBuilder.getType()).thenReturn("journal");
        when(slingSettings.getSlingId()).thenReturn(SUB1_SLING_ID);
        when(clientProvider.getServerUri()).thenReturn(new URI("http://myserver.apache.org:1234/somepath"));
        when(clientProvider.<PackageStatusMessage>createSender(Topics.STATUS_TOPIC))
                .thenReturn(statusSender);
        when(clientProvider.<DiscoveryMessage>createSender(Topics.DISCOVERY_TOPIC))
                .thenReturn(discoverySender);
        when(precondition.canProcess(eq(SUB1_AGENT_NAME), anyLong())).thenReturn(Decision.ACCEPT);
    }

    @After
    public void after() {
        subscriber.deactivate();
        ((Logger) LoggerFactory.getLogger(DistributionSubscriber.class)).detachAppender(logAppender);
    }

    @Test
    public void testBacklogIsImportedAsBatch() throws Exception {
        recordInstalledBatches();
        CountDownLatch firstImportStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstImport = new CountDownLatch(1);
        when(packageBuilder.installPackage(any(ResourceResolver.class), any(DistributionPackage.class)))
                .thenAnswer(invocation -> {
                    firstImportStarted.countDown();
                    releaseFirstImport.await();
                    return true;
                });
        initSubscriber(3);

        // The importer picks up the first package alone and blocks in the single install
        packageHandler.handle(new TestMessageInfo("", 1, 0, 0), message("p0", ReqType.ADD));
        assertTrue(firstImportStarted.await(10, SECONDS));

        // Meanwhile the next packages queue up and are imported together
        packageHandler.handle(new TestMessageInfo("", 1, 1, 0), message("p1", ReqType.ADD));
        packageHandler.handle(new TestMessageInfo("", 1, 2, 0), message("p2", ReqType.DELETE));
        packageHandler.handle(new TestMessageInfo("", 1, 3, 0), message("p3", ReqType.ADD));
        releaseFirstImport.countDown();

        await().atMost(10, SECONDS).until(this::getStoredOffset, equalTo(3L));
        assertThat(installedBatches, contains(Arrays.asList("p1", "p2", "p3")));
        verify(packageBuilder, times(1)).installPackage(any(ResourceResolver.class), any(DistributionPackage.class));
    }

    @Test
    public void testFailedBatchFallsBackToSinglePackages() throws Exception {
        doThrow(new DistributionException("batch failed"))
                .when(packageBuilder)
                .installPackages(any(ResourceResolver.class), anyList());
        CountDownLatch firstImportStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstImport = new CountDownLatch(1);
        when(packageBuilder.installPackage(any(ResourceResolver.class), any(DistributionPackage.class)))
                .thenAnswer(invocation -> {
                    firstImportStarted.countDown();
                    releaseFirstImport.await();
                    return true;
                });
        initSubscriber(3);

        packageHandler.handle(new TestMessageInfo("", 1, 0, 0), message("p0", ReqType.ADD));
        assertTrue(firstImportStarted.await(10, SECONDS));
        packageHandler.handle(new TestMessageInfo("", 1, 1, 0), message("p1", ReqType.ADD));
        packageHandler.handle(new TestMessageInfo("", 1, 2, 0), message("p2", ReqType.ADD));
        releaseFirstImport.countDown();

        await().atMost(10, SECONDS).until(this::getStoredOffset, equalTo(2L));
        verify(packageBuilder, timeout(10000).times(1)).installPackages(any(ResourceResolver.class), anyList());
        verify(packageBuilder, times(3)).installPackage(any(ResourceResolver.class), any(DistributionPackage.class));
    }

    @Test
    public void testSkippedPackagesDoNotEndBatch() throws Exception {
        recordInstalledBatches();
        CountDownLatch firstImportStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstImport = new CountDownLatch(1);
        blockFirstSingleImport(firstImportStarted, releaseFirstImport);
        int skippedCount = BookKeeper.COMMIT_AFTER_NUM_SKIPPED + 1;
        int lastImportOffset = skippedCount + 2;
        initSubscriber(lastImportOffset + skippedCount);

        packageHandler.handle(new TestMessageInfo("", 1, 0, 0), message("p0", ReqType.ADD));
        assertTrue(firstImportStarted.await(10, SECONDS));
        packageHandler.handle(new TestMessageInfo("", 1, 1, 0), message("p1", ReqType.ADD));
        for (int offset = 2; offset < lastImportOffset; offset++) {
            packageHandler.handle(new TestMessageInfo("", 1, offset, 0), skippedMessage(offset, ReqType.INVALIDATE));
        }
        packageHandler.handle(new TestMessageInfo("", 1, lastImportOffset, 0), message("p2", ReqType.DELETE));
        for (int offset = lastImportOffset + 1; offset <= lastImportOffset + skippedCount; offset++) {
            packageHandler.handle(new TestMessageInfo("", 1, offset, 0), skippedMessage(offset, ReqType.ADD));
        }
        when(precondition.canProcess(SUB1_AGENT_NAME, lastImportOffset)).thenAnswer(invocation -> {
            assertThat(getStoredOffset(), equalTo(0L));
            return Decision.ACCEPT;
        });
        releaseFirstImport.countDown();

        await().atMost(10, SECONDS).until(this::getStoredOffset, equalTo((long) lastImportOffset + skippedCount));
        assertThat(installedBatches, contains(Arrays.asList("p1", "p2")));
        verify(packageBuilder, times(1)).installPackage(any(ResourceResolver.class), any(DistributionPackage.class));
        verifySkipWarnings(skippedCount - 1);
    }

    @Test
    public void testFailedInterleavedBatchFallsBackInJournalOrder() throws Exception {
        doThrow(new DistributionException("batch failed"))
                .when(packageBuilder)
                .installPackages(any(ResourceResolver.class), anyList());
        CountDownLatch firstImportStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstImport = new CountDownLatch(1);
        blockFirstSingleImport(firstImportStarted, releaseFirstImport);
        int lastImportOffset = BookKeeper.COMMIT_AFTER_NUM_SKIPPED + 3;
        initSubscriber(lastImportOffset);

        packageHandler.handle(new TestMessageInfo("", 1, 0, 0), message("p0", ReqType.ADD));
        assertTrue(firstImportStarted.await(10, SECONDS));
        packageHandler.handle(new TestMessageInfo("", 1, 1, 0), message("p1", ReqType.ADD));
        for (int offset = 2; offset < lastImportOffset; offset++) {
            packageHandler.handle(new TestMessageInfo("", 1, offset, 0), skippedMessage(offset, ReqType.ADD));
        }
        packageHandler.handle(new TestMessageInfo("", 1, lastImportOffset, 0), message("p2", ReqType.ADD));
        List<Long> offsetsBeforeSingleImports = Collections.synchronizedList(new ArrayList<>());
        doAnswer(invocation -> {
                    offsetsBeforeSingleImports.add(getStoredOffset());
                    return true;
                })
                .when(packageBuilder)
                .installPackage(any(ResourceResolver.class), any(DistributionPackage.class));
        releaseFirstImport.countDown();

        await().atMost(10, SECONDS).until(this::getStoredOffset, equalTo((long) lastImportOffset));
        assertThat(offsetsBeforeSingleImports, contains(0L, (long) BookKeeper.COMMIT_AFTER_NUM_SKIPPED + 2));
        verify(packageBuilder, times(1)).installPackages(any(ResourceResolver.class), anyList());
        verify(packageBuilder, times(3)).installPackage(any(ResourceResolver.class), any(DistributionPackage.class));
        verifySkipWarnings(BookKeeper.COMMIT_AFTER_NUM_SKIPPED / 2);
    }

    @Test
    public void testSkippedOnlyRunStillCheckpoints() throws Exception {
        CountDownLatch firstImportStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstImport = new CountDownLatch(1);
        blockFirstSingleImport(firstImportStarted, releaseFirstImport);
        int skippedCount = BookKeeper.COMMIT_AFTER_NUM_SKIPPED + 1;
        initSubscriber(skippedCount);

        packageHandler.handle(new TestMessageInfo("", 1, 0, 0), message("p0", ReqType.ADD));
        assertTrue(firstImportStarted.await(10, SECONDS));
        for (int offset = 1; offset <= skippedCount; offset++) {
            packageHandler.handle(new TestMessageInfo("", 1, offset, 0), skippedMessage(offset, ReqType.ADD));
        }
        releaseFirstImport.countDown();

        await().atMost(10, SECONDS).until(this::getStoredOffset, equalTo((long) skippedCount));
        verify(packageBuilder, times(1)).installPackage(any(ResourceResolver.class), any(DistributionPackage.class));
        verify(packageBuilder, never()).installPackages(any(ResourceResolver.class), anyList());
        verifySkipWarnings((skippedCount + 1) / 2);
    }

    @Test
    public void testInvalidateEndsBatch() throws Exception {
        recordInstalledBatches();
        CountDownLatch firstImportStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstImport = new CountDownLatch(1);
        when(packageBuilder.installPackage(any(ResourceResolver.class), any(DistributionPackage.class)))
                .thenAnswer(invocation -> {
                    firstImportStarted.countDown();
                    releaseFirstImport.await();
                    return true;
                });
        initSubscriber(10);

        packageHandler.handle(new TestMessageInfo("", 1, 0, 0), message("p0", ReqType.ADD));
        assertTrue(firstImportStarted.await(10, SECONDS));
        packageHandler.handle(new TestMessageInfo("", 1, 1, 0), message("p1", ReqType.ADD));
        packageHandler.handle(new TestMessageInfo("", 1, 2, 0), message("p2", ReqType.ADD));
        packageHandler.handle(new TestMessageInfo("", 1, 3, 0), message("p3", ReqType.INVALIDATE));
        packageHandler.handle(new TestMessageInfo("", 1, 4, 0), message("p4", ReqType.ADD));
        packageHandler.handle(new TestMessageInfo("", 1, 5, 0), message("p5", ReqType.ADD));
        releaseFirstImport.countDown();

        await().atMost(10, SECONDS).until(this::getStoredOffset, equalTo(5L));
        assertThat(installedBatches, contains(Arrays.asList("p1", "p2"), Arrays.asList("p4", "p5")));
    }

    @Test
    public void testEditableAgentImportsBatchAndSendsOneStatus() throws Exception {
        recordInstalledBatches();
        CountDownLatch firstImportStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstImport = new CountDownLatch(1);
        blockFirstSingleImport(firstImportStarted, releaseFirstImport);
        initEditableSubscriber(3);

        packageHandler.handle(new TestMessageInfo("", 1, 0, 0), message("p0", ReqType.ADD));
        assertTrue(firstImportStarted.await(10, SECONDS));
        packageHandler.handle(new TestMessageInfo("", 1, 1, 0), message("p1", ReqType.ADD));
        packageHandler.handle(new TestMessageInfo("", 1, 2, 0), message("p2", ReqType.DELETE));
        packageHandler.handle(new TestMessageInfo("", 1, 3, 0), message("p3", ReqType.ADD));
        releaseFirstImport.countDown();

        await().atMost(10, SECONDS).until(this::getStoredOffset, equalTo(3L));
        assertThat(installedBatches, contains(Arrays.asList("p1", "p2", "p3")));

        // One status for the single package p0 and one for the whole batch
        verify(statusSender, timeout(10000).times(2)).accept(statusCaptor.capture());
        PackageStatusMessage batchStatus = statusCaptor.getAllValues().get(1);
        assertThat(batchStatus.getStatus(), equalTo(Status.IMPORTED));
        assertThat(batchStatus.getOffset(), equalTo(3L));
    }

    @Test
    public void testClearedPackagesEndBatchOnEditableAgent() throws Exception {
        recordInstalledBatches();
        CountDownLatch firstImportStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstImport = new CountDownLatch(1);
        blockFirstSingleImport(firstImportStarted, releaseFirstImport);
        initEditableSubscriber(10);

        packageHandler.handle(new TestMessageInfo("", 1, 0, 0), message("p0", ReqType.ADD));
        assertTrue(firstImportStarted.await(10, SECONDS));
        packageHandler.handle(new TestMessageInfo("", 1, 1, 0), message("p1", ReqType.ADD));
        packageHandler.handle(new TestMessageInfo("", 1, 2, 0), message("p2", ReqType.ADD));
        packageHandler.handle(new TestMessageInfo("", 1, 3, 0), message("p3", ReqType.ADD));
        packageHandler.handle(new TestMessageInfo("", 1, 4, 0), message("p4", ReqType.ADD));
        commandHandler.handle(
                new TestMessageInfo("", 1, 0, 0),
                ClearCommand.builder()
                        .subSlingId(SUB1_SLING_ID)
                        .subAgentName(SUB1_AGENT_NAME)
                        .offset(2)
                        .build());
        releaseFirstImport.countDown();

        await().atMost(10, SECONDS).until(this::getStoredOffset, equalTo(4L));
        assertThat(installedBatches, contains(Arrays.asList("p3", "p4")));
        verify(statusSender, timeout(10000).times(4)).accept(statusCaptor.capture());
        List<Status> statuses = new ArrayList<>();
        statusCaptor.getAllValues().forEach(s -> statuses.add(s.getStatus()));
        assertThat(statuses, contains(Status.IMPORTED, Status.REMOVED, Status.REMOVED, Status.IMPORTED));
    }

    @Test
    public void testNoBatchingWhenPackageHandlingIsNotOff() throws Exception {
        initSubscriber(3, Collections.singletonMap("packageHandling", "Extract"));

        packageHandler.handle(new TestMessageInfo("", 1, 0, 0), message("p0", ReqType.ADD));
        packageHandler.handle(new TestMessageInfo("", 1, 1, 0), message("p1", ReqType.ADD));

        assertThat(getStoredOffset(), equalTo(1L));
        verify(packageBuilder, times(2)).installPackage(any(ResourceResolver.class), any(DistributionPackage.class));
        verify(packageBuilder, never()).installPackages(any(ResourceResolver.class), anyList());
    }

    private void verifySkipWarnings(int count) {
        verify(logAppender, times(count))
                .doAppend(argThat(event -> event.getFormattedMessage().endsWith("(bad pkgType)")));
    }

    private void blockFirstSingleImport(CountDownLatch started, CountDownLatch release) throws DistributionException {
        when(packageBuilder.installPackage(any(ResourceResolver.class), any(DistributionPackage.class)))
                .thenAnswer(invocation -> {
                    started.countDown();
                    release.await();
                    return true;
                });
    }

    private void initEditableSubscriber(int maxBatchSize) {
        when(clientProvider.createPoller(
                        Mockito.eq(Topics.COMMAND_TOPIC), Mockito.eq(Reset.earliest), commandCaptor.capture()))
                .thenReturn(poller);
        initSubscriber(maxBatchSize, Collections.singletonMap("editable", "true"));
        commandHandler = commandCaptor.getValue().getHandler();
    }

    private void recordInstalledBatches() throws DistributionException {
        doAnswer(invocation -> {
                    List<DistributionPackage> packages = invocation.getArgument(1);
                    List<String> ids = new ArrayList<>();
                    for (DistributionPackage pkg : packages) {
                        ids.add(pkg.getId());
                    }
                    installedBatches.add(ids);
                    return null;
                })
                .when(packageBuilder)
                .installPackages(any(ResourceResolver.class), anyList());
    }

    private static PackageMessage message(String pkgId, ReqType reqType) {
        return message(pkgId, reqType, PUB1_AGENT_NAME, "journal");
    }

    private static PackageMessage skippedMessage(int offset, ReqType reqType) {
        return offset % 2 == 0
                ? message("other" + offset, reqType, "other-agent", "journal")
                : message("other" + offset, reqType, PUB1_AGENT_NAME, "other-type");
    }

    private static PackageMessage message(String pkgId, ReqType reqType, String pubAgentName, String pkgType) {
        return PackageMessage.builder()
                .pkgId(pkgId)
                .pubSlingId("pub1sling")
                .pubAgentName(pubAgentName)
                .reqType(reqType)
                .pkgType(pkgType)
                .paths(Collections.singletonList("/content/" + pkgId))
                .pkgBinary(new byte[10])
                .build();
    }

    private Long getStoredOffset() {
        LocalStore store = new LocalStore(resolverFactory, STORE_PACKAGE_NODE_NAME, SUB1_AGENT_NAME);
        return store.load(BookKeeper.KEY_OFFSET, Long.class);
    }

    private void initSubscriber(int maxBatchSize) {
        initSubscriber(maxBatchSize, Collections.emptyMap());
    }

    private void initSubscriber(int maxBatchSize, Map<String, String> overrides) {
        Map<String, Object> props = new HashMap<>();
        props.put("name", SUB1_AGENT_NAME);
        props.put("agentNames", PUB1_AGENT_NAME);
        props.put("maxBatchSize", maxBatchSize);
        props.putAll(overrides);
        SubscriberConfiguration config =
                Converters.standardConverter().convert(props).to(SubscriberConfiguration.class);
        subscriber = new DistributionSubscriber(
                packageBuilder,
                slingSettings,
                clientProvider,
                precondition,
                metricsService,
                bookKeeperFactory,
                subscriberReadyStore,
                new OnlyOnLeader(context),
                config,
                context,
                props);
        verify(clientProvider)
                .createPoller(
                        Mockito.eq(Topics.PACKAGE_TOPIC),
                        Mockito.eq(Reset.latest),
                        Mockito.isNull(String.class),
                        packageCaptor.capture(),
                        pingCaptor.capture());
        packageHandler = packageCaptor.getValue().getHandler();
    }
}
