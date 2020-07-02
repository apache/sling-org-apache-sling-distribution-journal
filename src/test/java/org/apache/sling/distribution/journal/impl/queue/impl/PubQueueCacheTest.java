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
package org.apache.sling.distribution.journal.impl.queue.impl;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.LocalStore;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class PubQueueCacheTest {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private static final String TOPIC = "package_topic";

    private static final String PUB_AGENT_NAME_1 = "pubAgentName1";

    private static final String PUB_AGENT_NAME_2 = "pubAgentName2";

    private static final String PUB_AGENT_NAME_3 = "pubAgentName3";

    private static final Random RAND = new Random();

    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageMessage>> handlerCaptor;

    @Captor
    private ArgumentCaptor<String> headAssignCaptor;

    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private MessagingProvider clientProvider;

    @Mock
    private QueueCacheSeeder cacheSeeder;

    @Mock
    private DistributionMetricsService distributionMetricsService;

    @Mock
    private Counter counter;

    @Mock
    private LocalStore seedStore;

    @Mock
    private Closeable poller;

    private PubQueueCache cache;

    private ExecutorService executor;

    private MessageHandler<PackageMessage> tailHandler;

    @Before
    public void before() {
        when(clientProvider.assignTo(anyLong())).then(
                answer -> "0:" + answer.getArguments()[0]);
        when(clientProvider.createPoller(
                eq(TOPIC),
                any(Reset.class),
                headAssignCaptor.capture(),
                handlerCaptor.capture()))
                .thenReturn(poller);

        when(distributionMetricsService.getQueueCacheFetchCount())
                .thenReturn(counter);

        when(seedStore.load(anyString(), any())).thenReturn(0L);

        cache = new PubQueueCache(clientProvider, eventAdmin, distributionMetricsService, TOPIC, seedStore, cacheSeeder);
        cache.storeSeed();

        executor = Executors.newFixedThreadPool(10);
        tailHandler = handlerCaptor.getValue().getHandler();
    }

    @After
    public void after() throws IOException {
        executor.shutdownNow();
        cache.close();
    }
    
    @Test(expected = RuntimeException.class)
    public void testUnseededThrows() throws Exception {
        cache.getOffsetQueue(PUB_AGENT_NAME_1, 0);
    }

    @Test
    public void testSeedingFromNewPackageMessage() throws Exception {
        simulateMessage(tailHandler, PUB_AGENT_NAME_1, ReqType.ADD, 0);
        OffsetQueue<DistributionQueueItem> queue = cache.getOffsetQueue(PUB_AGENT_NAME_1, 0);
        assertThat(queue.getSize(), greaterThan(0));
    }

    @Test
    public void testFetchWithSingleConsumer() throws Exception {
        simulateMessage(tailHandler, 200);
        Future<OffsetQueue<DistributionQueueItem>> consumer = consumer(PUB_AGENT_NAME_1, 100);
        // seeding the cache with a message at offset 200
        // wait that the consumer has started fetching the offsets from 100 to 200
        awaitHeadHandler();
        // simulate messages for the fetched offsets
        long fromOffset = offsetFromAssign(headAssignCaptor.getValue());
        simulateMessages(handlerCaptor.getValue().getHandler(), fromOffset, cache.getMinOffset());
        // the consumer returns the offset queue
        consumer.get(15, SECONDS);
        assertEquals(100, cache.getMinOffset());
    }

    private MessageHandler<PackageMessage> awaitHeadHandler() {
        return Awaitility.await().ignoreExceptions().until(() -> handlerCaptor.getAllValues().get(1).getHandler(), notNullValue());
    }

	@Test
    public void testFetchWithConcurrentConsumer() throws Exception {
	    simulateMessage(tailHandler, 200);
        // build two consumers for same agent queue, from offset 100
        Future<OffsetQueue<DistributionQueueItem>> consumer1 = consumer(PUB_AGENT_NAME_1, 100);
        Future<OffsetQueue<DistributionQueueItem>> consumer2 = consumer(PUB_AGENT_NAME_1, 100);
        // seeding the cache with a message at offset 200
        // wait that one consumer has started fetching the offsets from 100 to 200
        MessageHandler<PackageMessage> headHandler = awaitHeadHandler();
        // simulate messages for the fetched offsets
        long fromOffset = offsetFromAssign(headAssignCaptor.getValue());
        simulateMessages(headHandler, fromOffset, cache.getMinOffset());
        // both consumers returns the offset queue
        OffsetQueue<DistributionQueueItem> q1 = consumer1.get(5, SECONDS);
        OffsetQueue<DistributionQueueItem> q2 = consumer2.get(5, SECONDS);
        assertEquals(q1.getSize(), q2.getSize());
        assertEquals(100, cache.getMinOffset());
        // the offsets have been fetched only once
        assertEquals(2, handlerCaptor.getAllValues().size());
    }

    @Test
    public void testCacheSize() throws Exception {
        simulateMessage(tailHandler, PUB_AGENT_NAME_3, ReqType.ADD, 0);
        simulateMessage(tailHandler, PUB_AGENT_NAME_3, ReqType.DELETE, 1);
        simulateMessage(tailHandler, PUB_AGENT_NAME_1, ReqType.ADD, 2);
        simulateMessage(tailHandler, PUB_AGENT_NAME_3, ReqType.TEST, 3);    // TEST message does not increase the cache size
        simulateMessage(tailHandler, PUB_AGENT_NAME_2, ReqType.TEST, 4);    // TEST message does not increase the cache size
        simulateMessage(tailHandler, PUB_AGENT_NAME_3, ReqType.ADD, 5);
        assertEquals(4, cache.size());
    }

    private void simulateMessages(MessageHandler<PackageMessage> handler, long fromOffset, long toOffset) {
        LongStream.rangeClosed(fromOffset, toOffset).forEach(offset -> simulateMessage(handler, offset));
    }
    
    private void simulateMessage(MessageHandler<PackageMessage> handler, long offset) {
        simulateMessage(handler,
                pickAny(PUB_AGENT_NAME_1, PUB_AGENT_NAME_2, PUB_AGENT_NAME_3),
                pickAny(ReqType.ADD, ReqType.DELETE, ReqType.TEST), offset);
    }

    private void simulateMessage(MessageHandler<PackageMessage> handler, String pubAgentName, ReqType reqType, long offset) {
        PackageMessage msg = PackageMessage.builder()
                .pkgType("pkgType")
                .pkgId(UUID.randomUUID().toString())
                .pubSlingId("pubSlingId")
                .reqType(reqType)
                .pubAgentName(pubAgentName)
                .build();
        simulateMessage(handler, msg, offset);
    }

    private void simulateMessage(MessageHandler<PackageMessage> handler, PackageMessage msg, long offset) {
        log.info("Simulate msg @ offset {}", offset);
        handler.handle(new TestMessageInfo(TOPIC, 0, offset, currentTimeMillis()), msg);
    }

    Future<OffsetQueue<DistributionQueueItem>> consumer(String pubAgentName, long minOffset) {
        return executor.submit(() -> cache.getOffsetQueue(pubAgentName, minOffset));
    }

    @SafeVarargs
    private final <T> T pickAny(T... c) {
        if (c == null || c.length == 0) {
            throw new IllegalArgumentException();
        }
        return c[RAND.nextInt(c.length)];
    }

    private Long offsetFromAssign(String assign) {
        String[] chunks = assign.split(":");
        return Long.parseLong(chunks[1]);
    }


}