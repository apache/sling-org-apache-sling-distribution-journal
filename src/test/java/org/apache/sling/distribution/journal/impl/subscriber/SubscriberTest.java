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
import static org.apache.sling.distribution.agent.DistributionAgentState.RUNNING;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_PACKAGE_ID;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_PATHS;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_TYPE;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.distribution.ImportPostProcessException;
import org.apache.sling.distribution.ImportPostProcessor;
import org.apache.sling.distribution.ImportPreProcessException;
import org.apache.sling.distribution.ImportPreProcessor;
import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeper;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperFactory;
import org.apache.sling.distribution.journal.bookkeeper.LocalStore;
import org.apache.sling.distribution.journal.bookkeeper.SubscriberMetrics;
import org.apache.sling.distribution.journal.shared.NoOpImportPreProcessor;
import org.apache.sling.distribution.journal.shared.NoOpImportPostProcessor;
import org.apache.sling.distribution.journal.impl.precondition.Precondition;
import org.apache.sling.distribution.journal.impl.precondition.Precondition.Decision;
import org.apache.sling.distribution.journal.messages.ClearCommand;
import org.apache.sling.distribution.journal.messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.messages.PingMessage;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.packaging.DistributionPackageInfo;
import org.apache.sling.settings.SlingSettingsService;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.EventAdmin;
import org.osgi.util.converter.Converters;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SubscriberTest {

    private static final String SUB1_SLING_ID = "sub1sling";
    private static final String SUB1_AGENT_NAME = "sub1agent";
    
    private static final String PUB1_SLING_ID = "pub1sling";
    private static final String PUB1_AGENT_NAME = "pub1agent";
    
    private static final String STORE_PACKAGE_NODE_NAME = "myserver.apache.org_somepath_aemdistribution_package";

    private static final PackageMessage BASIC_ADD_PACKAGE = PackageMessage.builder()
            .pkgId("myid")
            .pubSlingId(PUB1_SLING_ID)
            .pubAgentName(PUB1_AGENT_NAME)
            .reqType(ReqType.ADD)
            .pkgType("journal")
            .paths(Arrays.asList("/test"))
            .pkgBinary(new byte[100])
            .build();

    private static final PackageMessage BASIC_DEL_PACKAGE = PackageMessage.builder()
            .pkgId("myid")
            .pubSlingId(PUB1_SLING_ID)
            .pubAgentName(PUB1_AGENT_NAME)
            .reqType(ReqType.DELETE)
            .pkgType("journal")
            .paths(Arrays.asList("/test"))
            .build();


    
    @Mock
    private BundleContext context;

    @Mock
    private DistributionPackageBuilder packageBuilder;

    @Mock
    private Precondition precondition;

    @Mock
    private SlingSettingsService slingSettings;

    @Spy
    private ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();
    
    @Mock
    MessagingProvider clientProvider;
    
    @Spy
    Topics topics = new Topics();

    @Mock
    EventAdmin eventAdmin;
    
    @Mock
    private ResourceResolver resourceResolver;
    
    @Mock
    private MessageSender<DiscoveryMessage> discoverySender;

    @Mock
    private MessageSender<PackageStatusMessage> statusSender;

    @Spy
    private SubscriberMetrics subscriberMetrics = new SubscriberMetrics(MetricsService.NOOP);

    @Spy
    private ImportPreProcessor importPreProcessor = new NoOpImportPreProcessor();
    
    @Spy
    private ImportPostProcessor importPostProcessor = new NoOpImportPostProcessor();
    
    @Spy
    SubscriberReadyStore subscriberReadyStore = new SubscriberReadyStore();

    @InjectMocks
    BookKeeperFactory bookKeeperFactory;

    @InjectMocks
    DistributionSubscriber subscriber;
    
    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageMessage>> packageCaptor;
    
    @Captor
    private ArgumentCaptor<HandlerAdapter<PingMessage>> pingCaptor;
    
    @Captor
    private ArgumentCaptor<HandlerAdapter<ClearCommand>> commandCaptor;
    
    @Captor
    private ArgumentCaptor<PackageStatusMessage> statusMessageCaptor;

    @Mock
    private Closeable poller;
    
    @Mock
    private Closeable commandPoller;
    
    @Mock
    private ServiceRegistration<DistributionAgent> reg;
    
    private MessageHandler<PackageMessage> packageHandler;
    
    private MessageHandler<ClearCommand> commandHandler;

    @Before
    public void before() throws URISyntaxException {
        DistributionSubscriber.QUEUE_FETCH_DELAY_MILLIS = 100;
        DistributionSubscriber.RETRY_DELAY_MILLIS = 100;
        
        Awaitility.setDefaultPollDelay(Duration.ZERO);
        Awaitility.setDefaultPollInterval(Duration.ONE_HUNDRED_MILLISECONDS);
        when(packageBuilder.getType()).thenReturn("journal");
        when(slingSettings.getSlingId()).thenReturn(SUB1_SLING_ID);

        URI serverURI = new URI("http://myserver.apache.org:1234/somepath");
        when(clientProvider.getServerUri()).thenReturn(serverURI);
        when(clientProvider.<PackageStatusMessage>createSender(topics.getStatusTopic())).thenReturn(statusSender);
        when(clientProvider.<DiscoveryMessage>createSender(topics.getDiscoveryTopic())).thenReturn(discoverySender);

        when(clientProvider.createPoller(
                Mockito.eq(topics.getCommandTopic()),
                Mockito.eq(Reset.earliest), 
                commandCaptor.capture()))
            .thenReturn(commandPoller);
        
        // you should call initSubscriber in each test method
    }

    @After
    public void after() throws IOException {
        subscriber.deactivate();
        //verify(poller, atLeastOnce()).close();
    }
    
    @Test
    public void testReceiveNotSubscribed() throws DistributionException {
        assumeNoPrecondition();
        initSubscriber(Collections.singletonMap("agentNames", "dummy"));
        assertThat(subscriber.getState(), equalTo(DistributionAgentState.IDLE));
        
        MessageInfo info = createInfo(100);
        PackageMessage message = BASIC_ADD_PACKAGE;
        packageHandler.handle(info, message);
        
        verify(packageBuilder, timeout(1000).times(0)).installPackage(any(ResourceResolver.class), 
                any(ByteArrayInputStream.class));
        assertThat(getStoredOffset(), nullValue());
        for (int c=0; c < BookKeeper.COMMIT_AFTER_NUM_SKIPPED; c++) {
            packageHandler.handle(info, message);
        }
        assertThat(getStoredOffset(), equalTo(100l));
    }
    
    @Test
    public void testReceive() throws DistributionException {
        assumeNoPrecondition();
        initSubscriber();
        assertThat(subscriber.getState(), equalTo(DistributionAgentState.IDLE));

        final Semaphore sem = new Semaphore(0);
        whenInstallPackage()
            .thenAnswer(new WaitFor(sem));
        
        MessageInfo info = createInfo(0l);
        PackageMessage message = BASIC_ADD_PACKAGE;
        packageHandler.handle(info, message);
        
        waitSubscriber(RUNNING);
        sem.release();
        
        waitSubscriber(IDLE);
        verifyNoStatusMessageSent();
    }

    @Test
    public void testImportPreAndPostProcessInvoked() throws DistributionException, ImportPostProcessException, ImportPreProcessException {
        assumeNoPrecondition();
        initSubscriber();
        assertThat(subscriber.getState(), equalTo(DistributionAgentState.IDLE));
        final Semaphore sem = new Semaphore(0);
        whenInstallPackage()
            .thenAnswer(new WaitFor(sem));

        MessageInfo info = createInfo(0l);
        PackageMessage message = BASIC_ADD_PACKAGE;
        packageHandler.handle(info, message);

        waitSubscriber(RUNNING);
        sem.release();

        waitSubscriber(IDLE);
        verifyNoStatusMessageSent();
        
        Map<String, Object> props = new HashMap<>();
        props.put(DISTRIBUTION_TYPE, message.getReqType().name());
        props.put(DISTRIBUTION_PATHS,  message.getPaths());
        props.put(DISTRIBUTION_PACKAGE_ID, message.getPkgId());
        
        verify(importPreProcessor, times(1)).process(props);
        verify(importPostProcessor, times(1)).process(props);
    }

    @Test
    public void testImportPreProcessError() throws ImportPreProcessException {
        assumeNoPrecondition();
        initSubscriber(Collections.singletonMap("maxRetries", "0"));
        doThrow(new ImportPreProcessException("Failed pre process")).
                when(importPreProcessor).process(any());

        MessageInfo info = createInfo(0L);
        packageHandler.handle(info, BASIC_ADD_PACKAGE);

        verifyStatusMessageSentWithStatus(Status.REMOVED_FAILED);
    }

    @Test
    public void testImportPostProcessError() throws DistributionException, ImportPostProcessException {
        assumeNoPrecondition();
        initSubscriber(Collections.singletonMap("maxRetries", "0"));
        doThrow(new ImportPostProcessException("Failed post process")).
            when(importPostProcessor).process(any());

        MessageInfo info = createInfo(0l);
        PackageMessage message = BASIC_ADD_PACKAGE;
        packageHandler.handle(info, message);
        
        verifyStatusMessageSentWithStatus(Status.REMOVED_FAILED);
    }

    @Test
    public void testReceiveDelete() throws LoginException, PersistenceException {
        assumeNoPrecondition();
        initSubscriber();
        waitSubscriber(IDLE);
        createResource("/test");
        MessageInfo info = createInfo(0L);
        PackageMessage message = BASIC_DEL_PACKAGE;
        packageHandler.handle(info, message);
        waitSubscriber(IDLE);
        await().atMost(30, SECONDS).until(() -> getResource("/test") == null);
        verifyNoStatusMessageSent();
    }

    @Test
    public void testSendFailedStatus() throws DistributionException {
        assumeNoPrecondition();
        initSubscriber(Collections.singletonMap("maxRetries", "1"));
        whenInstallPackage()
        .thenThrow(new RuntimeException("Expected"));

        MessageInfo info = createInfo(0l);
        PackageMessage message = BASIC_ADD_PACKAGE;
        packageHandler.handle(info, message);
        
        verifyStatusMessageSentWithStatus(Status.REMOVED_FAILED);
    }

    @Test
    public void testSendSuccessStatus() throws DistributionException, InterruptedException {
        assumeNoPrecondition();
        // Only editable subscriber will send status
        initSubscriber(Collections.singletonMap("editable", "true"));

        MessageInfo info = createInfo(0l);
        PackageMessage message = BASIC_ADD_PACKAGE;
        packageHandler.handle(info, message);
        
        waitSubscriber(IDLE);
        verifyStatusMessageSentWithStatus(Status.IMPORTED);
    }

    @Test
    public void testSkipBecauseOfPrecondition() throws DistributionException, InterruptedException, TimeoutException {
        when(precondition.canProcess(eq(SUB1_AGENT_NAME), anyLong())).thenReturn(Decision.SKIP);
        initSubscriber(Collections.singletonMap("editable", "true"));

        MessageInfo info = createInfo(11l);
        PackageMessage message = BASIC_ADD_PACKAGE;
        packageHandler.handle(info, message);
        
        await().until(this::getStoredStatus, equalTo(PackageStatusMessage.Status.REMOVED));
        verifyStatusMessageSentWithStatus(Status.REMOVED);
    }
    
    @Test
    public void testPreconditionTimeoutExceptionBecauseOfShutdown() throws DistributionException, InterruptedException, TimeoutException, IOException {
        initSubscriber(Collections.singletonMap("editable", "true"));
        long startedAt = System.currentTimeMillis();

        MessageInfo info = createInfo(11l);
        PackageMessage message = BASIC_ADD_PACKAGE;
        packageHandler.handle(info, message);
        subscriber.deactivate();
        
        assertThat("After deactivate precondition should time out quickly.", System.currentTimeMillis() - startedAt, lessThan(1000l));
    }

    @Test
    public void testReadyWhenWatingForPrecondition() {
        Semaphore sem = new Semaphore(0);
        assumeWaitingForPrecondition(sem);
        initSubscriber();

        MessageInfo info = createInfo(0l);
        PackageMessage message = BASIC_ADD_PACKAGE;
        packageHandler.handle(info, message);

        waitSubscriber(RUNNING);
        await("Should report ready").until(() -> subscriberReadyStore.getReadyHolder(SUB1_AGENT_NAME).get());
        sem.release();
    }
    
    private void verifyNoStatusMessageSent() {
        verify(statusSender, times(0)).accept(any());
    }

    private PackageStatusMessage verifyStatusMessageSentWithStatus(Status expectedStatus) {
        verify(statusSender, timeout(10000).times(1)).accept(statusMessageCaptor.capture());
        PackageStatusMessage statusMessage = statusMessageCaptor.getValue();
        assertThat(statusMessage.getStatus(), equalTo(expectedStatus));
        return statusMessage;
    }

    private OngoingStubbing<DistributionPackageInfo> whenInstallPackage() throws DistributionException {
        return when(packageBuilder.installPackage(any(ResourceResolver.class), any(ByteArrayInputStream.class)));
    }

    private TestMessageInfo createInfo(long offset) {
        return new TestMessageInfo("", 1, offset, 0);
    }

    private Long getStoredOffset() {
        LocalStore store = new LocalStore(resolverFactory, STORE_PACKAGE_NODE_NAME, SUB1_AGENT_NAME);
        return store.load(BookKeeper.KEY_OFFSET, Long.class);
    }

    private Status getStoredStatus() {
        LocalStore statusStore = new LocalStore(resolverFactory, BookKeeper.STORE_TYPE_STATUS, SUB1_AGENT_NAME);
        return new BookKeeper.PackageStatus(statusStore.load()).status;
    }

    private void createResource(String path) throws PersistenceException, LoginException {
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            ResourceUtil.getOrCreateResource(resolver, path,"sling:Folder", "sling:Folder", true);
        }
    }

    private Resource getResource(String path) throws LoginException {
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            return resolver.getResource(path);
        }
    }

    private void initSubscriber() {
        initSubscriber(Collections.emptyMap());
    }

    private void initSubscriber(Map<String, String> overrides) {
        Map<String, Object> basicPropsOrig = new HashMap<>();
        basicPropsOrig.put("name", SUB1_AGENT_NAME);
        basicPropsOrig.put("agentNames", PUB1_AGENT_NAME);
        basicPropsOrig.put("idleMillies", 1000);
        basicPropsOrig.put("subscriberIdleCheck", true);
        Map<String, Object> basicProps = Collections.unmodifiableMap(basicPropsOrig);
        Map<String, Object> props = new HashMap<>();
        props.putAll(basicProps);
        props.putAll(overrides);
        SubscriberConfiguration config = Converters.standardConverter().convert(props).to(SubscriberConfiguration.class);
        subscriber.bookKeeperFactory = bookKeeperFactory;
        subscriber.activate(config, context, props);
        verify(clientProvider).createPoller(
                Mockito.eq(topics.getPackageTopic()),
                Mockito.eq(Reset.latest), 
                Mockito.isNull(String.class),
                packageCaptor.capture(),
                pingCaptor.capture());
        packageHandler = packageCaptor.getValue().getHandler();
        if ("true".equals(props.get("editable"))) {
            commandHandler = commandCaptor.getValue().getHandler();
        }
    }

    private void waitSubscriber(DistributionAgentState expectedState) {
        await().atMost(30, SECONDS).until(subscriber::getState, equalTo(expectedState));
    }
    
    private void assumeNoPrecondition() {
        try {
            when(precondition.canProcess(eq(SUB1_AGENT_NAME), anyLong())).thenReturn(Decision.ACCEPT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assumeWaitingForPrecondition(Semaphore sem) {
        try {
            when(precondition.canProcess(eq(SUB1_AGENT_NAME), anyLong()))
                .thenAnswer(invocation -> sem.tryAcquire(10000, TimeUnit.SECONDS) ? Decision.ACCEPT : Decision.SKIP);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private static final class WaitFor implements Answer<DistributionPackageInfo> {
        private final Semaphore sem;
    
        private WaitFor(Semaphore sem) {
            this.sem = sem;
        }
    
        @Override
        public DistributionPackageInfo answer(InvocationOnMock invocation) throws Throwable {
            sem.acquire();
            return new DistributionPackageInfo("");
        }
    }
}
