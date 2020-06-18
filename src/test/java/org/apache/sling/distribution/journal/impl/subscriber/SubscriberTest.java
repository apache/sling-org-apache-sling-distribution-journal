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

import static org.apache.sling.distribution.agent.DistributionAgentState.IDLE;
import static org.apache.sling.distribution.agent.DistributionAgentState.RUNNING;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.sling.distribution.journal.impl.precondition.Precondition;
import org.apache.sling.distribution.journal.impl.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.impl.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestState;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.packaging.DistributionPackageInfo;
import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.apache.sling.distribution.queue.DistributionQueueItemState;
import org.apache.sling.distribution.queue.DistributionQueueState;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.apache.sling.settings.SlingSettingsService;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.EventAdmin;
import org.osgi.util.converter.Converters;

import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import com.google.common.collect.ImmutableMap;

@SuppressWarnings("unchecked")
public class SubscriberTest {

    private static final String SUB1_SLING_ID = "sub1sling";
    private static final String SUB1_AGENT_NAME = "sub1agent";
    
    private static final String PUB1_SLING_ID = "pub1sling";
    private static final String PUB1_AGENT_NAME = "pub1agent";

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

    @Mock
    private DistributionMetricsService distributionMetricsService;
    
    @Spy
    SubscriberReadyStore subscriberReadyStore = new SubscriberReadyStore();

    @InjectMocks
    DistributionSubscriber subscriber;
    
    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageMessage>> packageCaptor;

    @Mock
    private Closeable poller;
    
    @Mock
    private ServiceRegistration<DistributionAgent> reg;
    
    private MessageHandler<PackageMessage> packageHandler;


    @Before
    public void before() {
        DistributionSubscriber.QUEUE_FETCH_DELAY = 100;
        DistributionSubscriber.RETRY_DELAY = 100;
        
        Awaitility.setDefaultPollDelay(Duration.ZERO);
        Awaitility.setDefaultPollInterval(Duration.ONE_HUNDRED_MILLISECONDS);
        MockitoAnnotations.initMocks(this);
        when(packageBuilder.getType()).thenReturn("journal");
        when(slingSettings.getSlingId()).thenReturn(SUB1_SLING_ID);

        mockMetrics();

        when(clientProvider.<PackageStatusMessage>createSender(Mockito.eq(topics.getStatusTopic()))).thenReturn(statusSender);
        when(clientProvider.<DiscoveryMessage>createSender(Mockito.eq(topics.getDiscoveryTopic()))).thenReturn(discoverySender);

        when(clientProvider.createPoller(
                Mockito.anyString(),
                Mockito.eq(Reset.earliest), 
                Mockito.anyString(),
                packageCaptor.capture()))
            .thenReturn(poller);
        when(context.registerService(Mockito.any(Class.class), eq(subscriber), Mockito.any(Dictionary.class))).thenReturn(reg);

        // you should call initSubscriber in each test method
    }

    @After
    public void after() throws IOException {
        subscriber.deactivate();
        verify(poller).close();
    }
    
    @Test
    public void testReceive() throws DistributionException {
        assumeNoPrecondition();
        initSubscriber();

        assertThat(subscriber.getQueueNames(), contains(PUB1_AGENT_NAME));
        assertThat(subscriber.getQueue(PUB1_AGENT_NAME).getStatus().getState(), equalTo(DistributionQueueState.IDLE));
        assertThat(subscriber.getState(), equalTo(DistributionAgentState.IDLE));
        
        MessageInfo info = new TestMessageInfo("", 1, 0, 0);

        PackageMessage message = BASIC_ADD_PACKAGE;

        final Semaphore sem = new Semaphore(0);
        when(packageBuilder.installPackage(Mockito.any(ResourceResolver.class), 
                Mockito.any(ByteArrayInputStream.class))
                ).thenAnswer(new WaitFor(sem));
        packageHandler.handle(info, message);
        
        waitSubscriber(RUNNING);
        DistributionQueue queue = subscriber.getQueue(PUB1_AGENT_NAME);
        DistributionQueueEntry item = queue.getHead();
        assertThat(item.getStatus().getItemState(), equalTo(DistributionQueueItemState.QUEUED));
        
        sem.release();
        waitSubscriber(IDLE);
        verify(statusSender, times(0)).accept(anyObject());
        List<String> log = subscriber.getLog().getLines();
        // We do not use the DistributionLog anymore
        assertThat(log.size(), equalTo(0));
    }

	@Test
    public void testReceiveDelete() throws DistributionException, LoginException, PersistenceException {
        assumeNoPrecondition();
        initSubscriber();

        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            ResourceUtil.getOrCreateResource(resolver, "/test","sling:Folder", "sling:Folder", true);
        }
        MessageInfo info = new TestMessageInfo("", 1, 0, 0);

        PackageMessage message = BASIC_DEL_PACKAGE;

        packageHandler.handle(info, message);
        waitSubscriber(RUNNING);
        waitSubscriber(IDLE);
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            assertThat(resolver.getResource("/test"), nullValue());
        }
    }

    @Test
    public void testExecuteNotSupported() throws DistributionException {
        assumeNoPrecondition();
        initSubscriber();

        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "test");
        DistributionResponse response = subscriber.execute(resourceResolver, request);
        assertThat(response.getState(), equalTo(DistributionRequestState.DROPPED));
    }


    @Test
    public void testSendFailedStatus() throws DistributionException {
        assumeNoPrecondition();
        initSubscriber(ImmutableMap.of("maxRetries", "1"));

        MessageInfo info = new TestMessageInfo("", 1, 0, 0);
        PackageMessage message = BASIC_ADD_PACKAGE;

        when(packageBuilder.installPackage(Mockito.any(ResourceResolver.class),
                Mockito.any(ByteArrayInputStream.class))
        ).thenThrow(new RuntimeException("Expected"));

        packageHandler.handle(info, message);
        verify(statusSender, timeout(10000).times(1)).accept(anyObject());
    }

    @Test
    public void testSendSuccessStatus() throws DistributionException, InterruptedException {
        assumeNoPrecondition();
        initSubscriber(ImmutableMap.of("editable", "true"));

        MessageInfo info = new TestMessageInfo("", 1, 0, 0);
        PackageMessage message = BASIC_ADD_PACKAGE;

        packageHandler.handle(info, message);
        waitSubscriber(IDLE);

        verify(statusSender, timeout(10000).times(1)).accept(anyObject());
    }

    @Test
    public void testSkipOnRemovedStatus() throws DistributionException, InterruptedException, TimeoutException {
        assumeNoPrecondition();
        initSubscriber();
        MessageInfo info = new TestMessageInfo("", 1, 11, 0);
        PackageMessage message = BASIC_ADD_PACKAGE;

        packageHandler.handle(info, message);
        waitSubscriber(RUNNING);
        when(precondition.canProcess(eq(SUB1_AGENT_NAME), eq(11), anyInt())).thenReturn(false);

        try {
            waitSubscriber(IDLE);
            fail("Cannot be IDLE without a validation status");
        } catch (Throwable t) {

        }

        when(precondition.canProcess(eq(SUB1_AGENT_NAME), eq(11), anyInt())).thenReturn(true);
        waitSubscriber(IDLE);

    }
    
    @Test
    public void testReadyWhenWatingForPrecondition() {
        Semaphore sem = new Semaphore(0);
        assumeWaitingForPrecondition(sem);
        initSubscriber();
        MessageInfo info = new TestMessageInfo("", 1, 0, 0);
        PackageMessage message = BASIC_ADD_PACKAGE;

        packageHandler.handle(info, message);
        waitSubscriber(RUNNING);
        await("Should report ready").until(() -> subscriberReadyStore.getReadyHolder(SUB1_AGENT_NAME).get());
        sem.release();
    }

    private void initSubscriber() {
        initSubscriber(Collections.emptyMap());
    }

    private void initSubscriber(Map<String, String> overrides) {
        Map<String, Object> basicProps = ImmutableMap.of(
            "name", SUB1_AGENT_NAME,
            "agentNames", PUB1_AGENT_NAME,
            "idleMillies", 1000,
            "subscriberIdleCheck", true);
        Map<String, Object> props = new HashMap<>();
        props.putAll(basicProps);
        props.putAll(overrides);
        SubscriberConfiguration config = Converters.standardConverter().convert(props).to(SubscriberConfiguration.class);
        subscriber.activate(config, context, props);
        packageHandler = packageCaptor.getValue().getHandler();
    }

    private void waitSubscriber(DistributionAgentState expectedState) {
        await().until(subscriber::getState, equalTo(expectedState));
    }

    private void mockMetrics() {
        Histogram histogram = Mockito.mock(Histogram.class);
        Counter counter = Mockito.mock(Counter.class);
        Meter meter = Mockito.mock(Meter.class);
        Timer timer = Mockito.mock(Timer.class);
        Timer.Context timerContext = Mockito.mock(Timer.Context.class);
        when(timer.time())
            .thenReturn(timerContext);
        when(distributionMetricsService.getImportedPackageSize())
                .thenReturn(histogram);
        when(distributionMetricsService.getItemsBufferSize())
                .thenReturn(counter);
        when(distributionMetricsService.getFailedPackageImports())
                .thenReturn(meter);
        when(distributionMetricsService.getRemovedFailedPackageDuration())
                .thenReturn(timer);
        when(distributionMetricsService.getRemovedPackageDuration())
                .thenReturn(timer);
        when(distributionMetricsService.getImportedPackageDuration())
                .thenReturn(timer);
        when(distributionMetricsService.getSendStoredStatusDuration())
                .thenReturn(timer);
        when(distributionMetricsService.getProcessQueueItemDuration())
                .thenReturn(timer);
        when(distributionMetricsService.getPackageDistributedDuration())
                .thenReturn(timer);
    }

    private void assumeNoPrecondition() {
        try {
            when(precondition.canProcess(eq(SUB1_AGENT_NAME), anyLong(), anyInt())).thenReturn(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assumeWaitingForPrecondition(Semaphore sem) {
        try {
            when(precondition.canProcess(eq(SUB1_AGENT_NAME), anyLong(), anyInt()))
                .thenAnswer(invocation -> sem.tryAcquire(10000, TimeUnit.SECONDS));
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
