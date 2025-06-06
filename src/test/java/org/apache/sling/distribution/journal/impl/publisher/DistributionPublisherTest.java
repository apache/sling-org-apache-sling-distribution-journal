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

import static org.apache.sling.distribution.journal.impl.publisher.PublisherConfiguration.DEFAULT_QUEUE_SIZE_LIMIT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.internal.MetricsServiceImpl;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestState;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.discovery.DiscoveryService;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.queue.impl.OffsetQueueImpl;
import org.apache.sling.distribution.journal.queue.impl.PubQueue;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.service.condition.Condition;
import org.osgi.service.event.EventAdmin;
import org.osgi.util.converter.Converters;

@RunWith(MockitoJUnitRunner.class)
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
    private MessagingProvider messagingProvider;

    @Mock
    private PackageMessageFactory factory;

    @Mock
    private DistributionPackageBuilder packageBuilder;

    private OsgiContext context = new OsgiContext();

    private DistributionPublisher publisher;

    @Mock
    private ResourceResolver resourceResolver;

    @Mock
    private MessageSender<PackageMessage> sender;
    
    @Mock
    private PackageQueuedNotifier queuedNotifier;
    
    @Captor
    private ArgumentCaptor<PackageMessage> pkgCaptor;

    private MetricsService metricsService;

    @Before
    public void before() throws Exception {
        metricsService = context.registerInjectActivateService(MetricsServiceImpl.class);
        when(packageBuilder.getType()).thenReturn("journal");
        Map<String, String> props = Map.of("name", PUB1AGENT1,
                "maxQueueSizeDelay", "1000");
        PublisherConfiguration config = Converters.standardConverter().convert(props).to(PublisherConfiguration.class);

        BundleContext bcontext = context.bundleContext();
        when(messagingProvider.<PackageMessage>createSender(Mockito.anyString())).thenReturn(sender);
        publisher = new DistributionPublisher(messagingProvider, packageBuilder, discoveryService, factory,
                eventAdmin, metricsService, pubQueueProvider, Condition.INSTANCE, config, bcontext);
        when(pubQueueProvider.getQueuedNotifier()).thenReturn(queuedNotifier);
    }
    
    @After
    public void after() {
        publisher.deactivate();
    }
    
    @Test
    public void executeRequestADDAccepted() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "/test");
        StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        executeAndCheck(request);
        stopwatch.stop();
        long time = stopwatch.getTime(TimeUnit.MILLISECONDS);
        assertThat(time, lessThan(1000L));
    }
    
    @Test
    public void executeRequestDELETEAccepted() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.DELETE, "/test");
        executeAndCheck(request);
    }

    @Test
    public void executeRequestINVALIDATEAccepted() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.INVALIDATE, "/test");
        executeAndCheck(request);
    }

    @Test
    public void executeRequestTESTAccepted() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.TEST, "/test");
        executeAndCheck(request);
    }
    
    @Test
    public void testQueueSizeLimitHalf() throws IOException, DistributionException {
        int queueSize = DEFAULT_QUEUE_SIZE_LIMIT + DEFAULT_QUEUE_SIZE_LIMIT / 2;
        when(pubQueueProvider.getMaxQueueSize(PUB1AGENT1)).thenReturn(queueSize);
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "/test");
        long time = distribute(request);
        assertThat(time, greaterThanOrEqualTo(500L));
        assertThat(time, lessThanOrEqualTo(550L));
    }

    @Test
    public void testDoubleQueueSizeLimitReached() throws IOException, DistributionException {
        int queueSize = DEFAULT_QUEUE_SIZE_LIMIT * 2;
        when(pubQueueProvider.getMaxQueueSize(PUB1AGENT1)).thenReturn(queueSize);
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "/test");
        long time = distribute(request);
        assertThat(time, greaterThanOrEqualTo(1000L));
        assertThat(time, lessThanOrEqualTo(1050L));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testExecutePullUnsupported() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.PULL, "/test");
        DistributionResponse response = publisher.execute(resourceResolver, request);
        
        assertThat(response.getState(), equalTo(DistributionRequestState.DROPPED));
        assertEquals("", response.getDistributionInfo().getId());

        List<String> log = publisher.getLog().getLines();
        assertThat(log, contains(
                containsString("Started Publisher agent=pub1agent1"),
                containsString("not supported by this agent")));
    }
    
    @Test
    public void testQueueNames() throws DistributionException, IOException {
        when(pubQueueProvider.getQueueNames(PUB1AGENT1)).thenReturn(Collections.singleton(QUEUE_NAME));
        Iterable<String> names = publisher.getQueueNames();
        assertThat(names, contains(QUEUE_NAME));
    }

    @Test
    public void testQueueNamesWithErrorQueue() throws DistributionException, IOException {
        when(pubQueueProvider.getQueueNames(Mockito.eq(PUB1AGENT1)))
            .thenReturn(new HashSet<>(Arrays.asList(QUEUE_NAME, QUEUE_NAME + "-error")));
        Iterable<String> names = publisher.getQueueNames();
        assertThat(names, containsInAnyOrder(QUEUE_NAME + "-error", QUEUE_NAME));
    }

    @Test
    public void testGetQueue() throws DistributionException, IOException {
        when(pubQueueProvider.getQueue(PUB1AGENT1, QUEUE_NAME))
            .thenReturn(new PubQueue(QUEUE_NAME, new OffsetQueueImpl<>(), 0, null,null));
        DistributionQueue queue = publisher.getQueue(QUEUE_NAME);
        assertThat(queue, notNullValue());
    }
    
    @Test
    public void testGetErrorQueue() throws DistributionException, IOException {
        when(pubQueueProvider.getQueue(PUB1AGENT1, QUEUE_NAME + "-error"))
            .thenReturn(new PubQueue(QUEUE_NAME, new OffsetQueueImpl<>(), 0, null,null));
        DistributionQueue queue = publisher.getQueue(QUEUE_NAME + "-error");
        assertThat(queue, notNullValue());
    }

    @Test
    public void testGetWrongQueue() throws DistributionException, IOException {

        DistributionQueue queue = publisher.getQueue("i_am_not_a_queue");
        assertNull(queue);
        long count = getQueueAccessErrorCount();
        assertEquals("Wrong queue counter expected",1, count);
    }

    @Test
    public void testGetQueueErrorMetrics() throws DistributionException, IOException {
       when(pubQueueProvider.getQueue(Mockito.any(), Mockito.any()))
            .thenThrow(new RuntimeException("Error"));

        try {
            publisher.getQueue(QUEUE_NAME);
            fail("Expected exception not thrown");
        } catch (RuntimeException expectedException) {
        }
        long count = getQueueAccessErrorCount();
        assertEquals("Wrong getQueue error counter",1, count);
    }

    private long getQueueAccessErrorCount() {
        return new PublishMetrics(metricsService, PUB1AGENT1).getQueueAccessErrorCount().getCount();
    }

    @Test(expected = DistributionException.class)
    public void testEmptyPaths() throws Exception {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, new String[0]);
        publisher.execute(resourceResolver, request);
    }

    @Test
    public void testEmptyRequest() throws DistributionException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, new String[] { "/" });
        when(factory.create(any(), any(), anyString(), any())).thenReturn(null);

        DistributionResponse response = publisher.execute(resourceResolver, request);
        assertEquals(DistributionRequestState.DROPPED, response.getState());
        assertEquals("Empty request", response.getMessage());
    }

    private long distribute(DistributionRequest request) throws IOException, DistributionException {
        StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        executeAndCheck(request);
        stopwatch.stop();
        long time = stopwatch.getTime(TimeUnit.MILLISECONDS);
        return time;
    }

    @SuppressWarnings("unchecked")
    private void executeAndCheck(DistributionRequest request) throws IOException, DistributionException {
        PackageMessage pkg = mockPackage(request);
        when(factory.create(any(DistributionPackageBuilder.class),Mockito.eq(resourceResolver), anyString(), Mockito.eq(request))).thenReturn(pkg);
        CompletableFuture<Long> callback = CompletableFuture.completedFuture(-1L);
        when(queuedNotifier.registerWait(Mockito.eq(pkg.getPkgId()))).thenReturn(callback);
    
        DistributionResponse response = publisher.execute(resourceResolver, request);
        
        assertThat(response.getState(), equalTo(DistributionRequestState.ACCEPTED));
        assertEquals("myid", response.getDistributionInfo().getId());
        verify(sender).accept(pkgCaptor.capture());
        PackageMessage sent = pkgCaptor.getValue();
        // Individual fields are checks in factory
        assertThat(sent, notNullValue());
        
        List<String> log = publisher.getLog().getLines();
        assertThat(log, contains(
                containsString("Started Publisher agent=pub1agent1"),
                containsString("Request accepted")));
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

}
