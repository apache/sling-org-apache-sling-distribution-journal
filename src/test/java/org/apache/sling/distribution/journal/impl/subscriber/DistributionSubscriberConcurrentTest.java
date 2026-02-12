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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.sling.distribution.agent.DistributionAgentState.IDLE;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.distribution.ImportPostProcessor;
import org.apache.sling.distribution.ImportPreProcessor;
import org.apache.sling.distribution.journal.BinaryStore;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeper;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperFactory;
import org.apache.sling.distribution.journal.bookkeeper.LocalStore;
import org.apache.sling.distribution.journal.impl.precondition.Precondition;
import org.apache.sling.distribution.journal.impl.precondition.Precondition.Decision;
import org.apache.sling.distribution.journal.messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.shared.NoOpImportPostProcessor;
import org.apache.sling.distribution.journal.shared.NoOpImportPreProcessor;
import org.apache.sling.distribution.journal.shared.OnlyOnLeader;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
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
import org.mockito.stubbing.Answer;
import org.osgi.framework.BundleContext;
import org.osgi.service.event.EventAdmin;
import org.osgi.util.converter.Converters;

/**
 * Tests concurrent import of packages by DistributionSubscriber with multiple importer threads.
 * Uses explicit synchronization (CountDownLatch) so the test does not depend on timing.
 */
@RunWith(MockitoJUnitRunner.class)
public class DistributionSubscriberConcurrentTest {

    private static final String SUB_SLING_ID = "subsling";
    private static final String SUB_AGENT_NAME = "subagent";
    private static final String PUB_AGENT_NAME = "pubagent";
    private static final String STORE_PACKAGE_NODE_NAME = "myserver.apache.org_somepath_package";
    private static final int NUM_MESSAGES = 4;
    /** Use at least 2 and enough threads so all messages can enter the handler (block in mock) at once. */
    private static final int IMPORT_THREADS = 4;
    private static final long AWAIT_SECONDS = 30;

    @Mock
    private BundleContext context;
    @Mock
    private DistributionPackageBuilder packageBuilder;
    @Mock
    private Precondition precondition;
    @Mock
    private SlingSettingsService slingSettings;
    @Mock
    private BinaryStore binaryStore;
    @Spy
    private ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();
    @Mock
    private MessagingProvider clientProvider;
    @Mock
    private EventAdmin eventAdmin;
    @Mock
    private org.apache.sling.distribution.journal.MessageSender<DiscoveryMessage> discoverySender;
    @Mock
    private org.apache.sling.distribution.journal.MessageSender<PackageStatusMessage> statusSender;
    @Spy
    private MetricsService metricsService = MetricsService.NOOP;
    @Spy
    private ImportPreProcessor importPreProcessor = new NoOpImportPreProcessor();
    @Spy
    private ImportPostProcessor importPostProcessor = new NoOpImportPostProcessor();
    @Spy
    private SubscriberReadyStore subscriberReadyStore = new SubscriberReadyStore();
    @InjectMocks
    private BookKeeperFactory bookKeeperFactory;

    private DistributionSubscriber subscriber;
    private MessageHandler<PackageMessage> packageHandler;

    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageMessage>> packageCaptor;

    @Before
    public void before() throws URISyntaxException {
        when(packageBuilder.getType()).thenReturn("journal");
        when(slingSettings.getSlingId()).thenReturn(SUB_SLING_ID);
        URI serverURI = new URI("http://myserver.apache.org:1234/somepath");
        when(clientProvider.getServerUri()).thenReturn(serverURI);
        when(clientProvider.<PackageStatusMessage>createSender(Topics.STATUS_TOPIC)).thenReturn(statusSender);
        when(clientProvider.<DiscoveryMessage>createSender(Topics.DISCOVERY_TOPIC)).thenReturn(discoverySender);
    }

    @After
    public void after() {
        if (subscriber != null) {
            subscriber.deactivate();
        }
    }

    /**
     * Submits multiple package messages (ADD, ADD, DELETE, ADD) with at least 2 importer threads.
     * Uses latches so that: (1) we know when all messages have entered the handler,
     * (2) we release them in a chosen order (2, 0, 3, 1) to validate that the stored offset
     * only advances when all lower offsets have completed (no timing assumptions).
     */
    @Test
    public void testConcurrentImportMultiplePackageTypes() throws Exception {
        assumePreconditionAccept();
        initSubscriberWithConcurrentThreads(IMPORT_THREADS);

        CountDownLatch allEntered = new CountDownLatch(NUM_MESSAGES);
        CountDownLatch[] startLatches = new CountDownLatch[NUM_MESSAGES];
        CountDownLatch[] doneLatches = new CountDownLatch[NUM_MESSAGES];
        for (int i = 0; i < NUM_MESSAGES; i++) {
            startLatches[i] = new CountDownLatch(1);
            doneLatches[i] = new CountDownLatch(1);
        }

        doAnswer((Answer<Void>) invocation -> {
            DistributionPackage pkg = invocation.getArgument(1);
            String id = pkg.getId();
            int idx = Integer.parseInt(id.replace("pkg-", ""));
            allEntered.countDown();
            startLatches[idx].await();
            doneLatches[idx].countDown();
            return null;
        }).when(packageBuilder).installPackage(any(ResourceResolver.class), any(DistributionPackage.class));

        PackageMessage add0 = packageMessage("pkg-0", ReqType.ADD);
        PackageMessage add1 = packageMessage("pkg-1", ReqType.ADD);
        PackageMessage del2 = packageMessage("pkg-2", ReqType.DELETE);
        PackageMessage add3 = packageMessage("pkg-3", ReqType.ADD);

        MessageInfo info0 = createInfo(0);
        MessageInfo info1 = createInfo(1);
        MessageInfo info2 = createInfo(2);
        MessageInfo info3 = createInfo(3);

        // packageHandler is the handler the subscriber registered with the poller; calling it
        // invokes the subscriber's delegatePackageMessageToExecutor -> import executor -> handlePackageMessage path
        packageHandler.handle(info0, add0);
        packageHandler.handle(info1, add1);
        packageHandler.handle(info2, del2);
        packageHandler.handle(info3, add3);

        assertThat("All messages should enter installPackage", allEntered.await(AWAIT_SECONDS, SECONDS));

        assertThat(getStoredOffset(), equalTo(-1L));

        startLatches[2].countDown();
        assertThat("Offset 2 done", doneLatches[2].await(AWAIT_SECONDS, SECONDS));
        assertThat("Offset 2 completed first; stored offset must not advance (0,1 still in flight)", getStoredOffset(), equalTo(-1L));

        startLatches[0].countDown();
        assertThat("Offset 0 done", doneLatches[0].await(AWAIT_SECONDS, SECONDS));
        await().atMost(AWAIT_SECONDS, SECONDS).until(() -> getStoredOffset() == 0L);

        startLatches[3].countDown();
        assertThat("Offset 3 done", doneLatches[3].await(AWAIT_SECONDS, SECONDS));
        await().atMost(AWAIT_SECONDS, SECONDS).until(() -> getStoredOffset() == 0L);

        startLatches[1].countDown();
        assertThat("Offset 1 done", doneLatches[1].await(AWAIT_SECONDS, SECONDS));
        await().atMost(AWAIT_SECONDS, SECONDS).until(() -> getStoredOffset() == 1L);

        waitSubscriberIdle();
        assertThat("Subscriber should be IDLE after all concurrent imports complete", subscriber.getState(), equalTo(IDLE));

        verify(packageBuilder, times(NUM_MESSAGES)).installPackage(any(ResourceResolver.class), any(DistributionPackage.class));
    }

    private void assumePreconditionAccept() {
        try {
            when(precondition.canProcess(Mockito.eq(SUB_AGENT_NAME), Mockito.anyLong())).thenReturn(Decision.ACCEPT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initSubscriberWithConcurrentThreads(int threads) {
        Map<String, Object> props = new HashMap<>();
        props.put("name", SUB_AGENT_NAME);
        props.put("agentNames", PUB_AGENT_NAME);
        props.put("idleMillies", 1000);
        props.put("subscriberIdleCheck", false); // avoid SubscriberIdleCheck registration with context (can block on CI)
        props.put("concurrentImporterThreads", threads);

        SubscriberConfiguration config = Converters.standardConverter().convert(props).to(SubscriberConfiguration.class);
        OnlyOnLeader onlyOnLeader = new OnlyOnLeader(context);

        subscriber = new DistributionSubscriber(
                packageBuilder,
                slingSettings,
                clientProvider,
                precondition,
                metricsService,
                bookKeeperFactory,
                subscriberReadyStore,
                onlyOnLeader,
                config,
                context,
                props);

        verify(clientProvider).createPoller(
                Mockito.eq(Topics.PACKAGE_TOPIC),
                Mockito.eq(Reset.latest),
                Mockito.nullable(String.class),
                packageCaptor.capture(),
                Mockito.any());
        packageHandler = packageCaptor.getValue().getHandler();
    }

    private void waitSubscriberIdle() {
        await().atMost(AWAIT_SECONDS, SECONDS).until(() -> subscriber.getState() == IDLE);
    }

    private static PackageMessage packageMessage(String pkgId, ReqType reqType) {
        return PackageMessage.builder()
                .pkgId(pkgId)
                .pubAgentName(PUB_AGENT_NAME)
                .reqType(reqType)
                .pkgType("journal")
                .paths(Arrays.asList("/test"))
                .pkgBinary(new byte[1])
                .build();
    }

    private static MessageInfo createInfo(long offset) {
        return new TestMessageInfo("", 0, offset, System.currentTimeMillis());
    }

    private long getStoredOffset() {
        LocalStore store = new LocalStore(resolverFactory, STORE_PACKAGE_NODE_NAME, SUB_AGENT_NAME);
        Long value = store.load(BookKeeper.KEY_OFFSET, Long.class);
        return value != null ? value : -1L;
    }
}
