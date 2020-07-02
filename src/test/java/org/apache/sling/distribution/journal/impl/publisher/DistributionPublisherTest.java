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
package org.apache.sling.distribution.journal.impl.publisher;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestState;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.apache.sling.settings.SlingSettingsService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.EventAdmin;
import org.osgi.util.converter.Converters;

import com.google.common.collect.ImmutableMap;

public class DistributionPublisherTest {

    private static final String SUBAGENT1 = "subscriber-agent1";
    private static final String QUEUE_NAME = "5eb78e0f-e8a2-4589-97dd-21649b37a0da-" + SUBAGENT1;

	private static final String PUB1AGENT1 = "pub1agent1";

	@Mock
    private EventAdmin eventAdmin;
    
    @Mock
    private DiscoveryService discoveryService;
    
    @Mock
    private PubQueueProvider pubQueueProvider;
    
    @Mock
    private SlingSettingsService slingSettings;
    
    @Mock
    private MessagingProvider messagingProvider;

    @Mock
    private PackageMessageFactory factory;

    @Mock
    private DistributionPackageBuilder packageBuilder;

    @Mock
    private DistributionMetricsService distributionMetricsService;

    @Mock
    private Histogram histogram;

    @Mock
    private Meter meter;

    @Mock
    private Timer timer;

    @Mock
    private Timer.Context timerContext;

    @Mock
    private BundleContext context;

    @InjectMocks
    private DistributionPublisher publisher;

    @Mock
    private ServiceRegistration<DistributionAgent> serviceReg;

    @Mock
    private ResourceResolver resourceResolver;

    @Mock
    private MessageSender<PackageMessage> sender;
    
    @Mock
    private PackageQueuedNotifier queuedNotifier;
    
    @Mock
    private TopologyView topology;
    
    @Captor
    private ArgumentCaptor<PackageMessage> pkgCaptor;

    @Spy
    private Topics topics = new Topics();

    @SuppressWarnings("unchecked")
    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        when(packageBuilder.getType()).thenReturn("journal");
        Map<String, String> props = ImmutableMap.of("name", PUB1AGENT1);
        PublisherConfiguration config = Converters.standardConverter().convert(props).to(PublisherConfiguration.class);
        when(slingSettings.getSlingId()).thenReturn("pub1sling");
        when(context.registerService(Mockito.eq(DistributionAgent.class), Mockito.eq(publisher),
                Mockito.any(Dictionary.class))).thenReturn(serviceReg);
        when(messagingProvider.<PackageMessage>createSender(Mockito.anyString())).thenReturn(sender);
        publisher.activate(config, context);
        when(timer.time()).thenReturn(timerContext);
    }
    
    @After
    public void after() {
        publisher.deactivate();
        verify(serviceReg).unregister();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testSend() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "/test");

        PackageMessage pkg = mockPackage(request);
        when(factory.create(Matchers.any(DistributionPackageBuilder.class),Mockito.eq(resourceResolver), anyString(), Mockito.eq(request))).thenReturn(pkg);
        CompletableFuture<Void> callback = CompletableFuture.completedFuture(null);
        when(queuedNotifier.registerWait(Mockito.eq(pkg.getPkgId()))).thenReturn(callback);
        when(distributionMetricsService.getExportedPackageSize()).thenReturn(histogram);
        when(distributionMetricsService.getAcceptedRequests()).thenReturn(meter);
        when(distributionMetricsService.getDroppedRequests()).thenReturn(meter);
        when(distributionMetricsService.getBuildPackageDuration()).thenReturn(timer);
        when(distributionMetricsService.getEnqueuePackageDuration()).thenReturn(timer);

        DistributionResponse response = publisher.execute(resourceResolver, request);
        
        assertThat(response.getState(), equalTo(DistributionRequestState.ACCEPTED));
        verify(sender).accept(pkgCaptor.capture());
        PackageMessage sent = pkgCaptor.getValue();
        // Individual fields are checks in factory
        assertThat(sent, notNullValue());
        
        List<String> log = publisher.getLog().getLines();
        assertThat(log, contains(
                containsString("Started Publisher agent pub1agent1"),
                containsString("Distribution request accepted")));
    }
    
    @SuppressWarnings("unchecked")
	@Test
    public void testSendUnsupported() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.PULL, "/test");
        DistributionResponse response = publisher.execute(resourceResolver, request);
        
        assertThat(response.getState(), equalTo(DistributionRequestState.DROPPED));
        
        List<String> log = publisher.getLog().getLines();
        assertThat(log, contains(
        		containsString("Started Publisher agent pub1agent1"),
                containsString("Request type PULL is not supported by this agent, expected one of")));
    }
    
    @Test
    public void testQueueNames() throws DistributionException, IOException {
        when(discoveryService.getTopologyView()).thenReturn(topology);
        when(topology.getSubscribedAgentIds(PUB1AGENT1)).thenReturn(Collections.singleton(QUEUE_NAME));
        State state = stateWithMaxRetries(-1);
        when(topology.getState(QUEUE_NAME, PUB1AGENT1)).thenReturn(state);
        Iterable<String> names = publisher.getQueueNames();
        assertThat(names, contains(QUEUE_NAME));
    }

    @Test
    public void testQueueNamesWithErrorQueue() throws DistributionException, IOException {
        when(discoveryService.getTopologyView()).thenReturn(topology);
        when(topology.getSubscribedAgentIds(PUB1AGENT1)).thenReturn(Collections.singleton(QUEUE_NAME));
        State state = new State(PUB1AGENT1, SUBAGENT1, 0, 1, 0, 1, false);
        when(topology.getState(QUEUE_NAME, PUB1AGENT1)).thenReturn(state);
        Iterable<String> names = publisher.getQueueNames();
        assertThat(names, containsInAnyOrder(QUEUE_NAME + "-error", QUEUE_NAME));
    }

    @Test
    public void testGetQueue() throws DistributionException, IOException {
        when(discoveryService.getTopologyView()).thenReturn(topology);
        when(topology.getSubscribedAgentIds(PUB1AGENT1)).thenReturn(Collections.singleton(QUEUE_NAME));
        State state = stateWithMaxRetries(1);
        when(topology.getState(QUEUE_NAME, PUB1AGENT1)).thenReturn(state);
        publisher.getQueue(QUEUE_NAME);
        publisher.getQueue(QUEUE_NAME + "-error");
        // TODO Add assertions
    }

    @Test
    public void testGetWrongQueue() throws DistributionException, IOException {
        when(discoveryService.getTopologyView()).thenReturn(topology);
        when(topology.getSubscribedAgentIds(PUB1AGENT1)).thenReturn(Collections.singleton(QUEUE_NAME));
        Counter counter = new TestCounter();
        when(distributionMetricsService.getQueueAccessErrorCount()).thenReturn(counter);

        DistributionQueue queue = publisher.getQueue("i_am_not_a_queue");
        assertNull(queue);
        assertEquals("Wrong queue counter expected",1, counter.getCount());
    }

    @Test
    public void testGetQueueErrorMetrics() throws DistributionException, IOException {
        when(discoveryService.getTopologyView()).thenReturn(topology);
        when(topology.getSubscribedAgentIds(PUB1AGENT1)).thenReturn(Collections.singleton(QUEUE_NAME));
        State state = stateWithMaxRetries(1);
        when(topology.getState(QUEUE_NAME, PUB1AGENT1)).thenReturn(state);
        AgentId subAgentId = new AgentId(QUEUE_NAME);
        when(pubQueueProvider.getQueue(PUB1AGENT1, subAgentId.getSlingId(), subAgentId.getAgentName(), QUEUE_NAME, 2, 0, false))
            .thenThrow(new RuntimeException("Error"));

        Counter counter = new TestCounter();
        when(distributionMetricsService.getQueueAccessErrorCount()).thenReturn(counter);
        try {
            publisher.getQueue(QUEUE_NAME);
            fail("Expected exception not thrown");
        } catch (RuntimeException expectedException) {
        }
        assertEquals("Wrong getQueue error counter",1, counter.getCount());
    }

    private State stateWithMaxRetries(int maxRetries) {
        return new State(PUB1AGENT1, SUBAGENT1, 0, 1, 0, maxRetries, false);
    }

    private PackageMessage mockPackage(DistributionRequest request) throws IOException {
        return PackageMessage.builder()
                .pkgId("myid")
                .pubSlingId("pub1sling")
                .pkgType("journal")
                .reqType(PackageMessage.ReqType.ADD)
                .paths(Arrays.asList(request.getPaths()))
                .deepPaths(Arrays.asList("/test2"))
                .pkgBinary(new byte[100])
                .build();
    }

    class TestCounter implements Counter {
        AtomicLong l = new AtomicLong();
        @Override public void increment() {
            l.getAndIncrement();
        }

        @Override public void decrement() {
            l.decrementAndGet();
        }

        @Override public void increment(long n) {
            l.addAndGet(n);
        }

        @Override public void decrement(long n) {
            l.addAndGet(-n);
        }

        @Override public long getCount() {
            return l.get();
        }

        @Override public <A> A adaptTo(Class<A> type) {
            return null;
        }
    };
    
}
