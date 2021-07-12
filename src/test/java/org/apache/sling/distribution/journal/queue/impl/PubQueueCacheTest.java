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
package org.apache.sling.distribution.journal.queue.impl;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.queue.CacheCallback;
import org.apache.sling.distribution.journal.queue.OffsetQueue;
import org.apache.sling.distribution.journal.queue.QueuedCallback;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
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
    private ArgumentCaptor<MessageHandler<PackageMessage>> handlerCaptor;

    @Mock
    private QueuedCallback queuedCallback;

    @Mock
    private CacheCallback callback;

    @Mock
    private Counter counter;

    @Mock
    private Closeable poller;

    private PubQueueCache cache;

    private ExecutorService executor;

    private MessageHandler<PackageMessage> tailHandler;


    @Before
    public void before() {
        when(callback.createConsumer(handlerCaptor.capture()))
                .thenReturn(poller);

        cache = new PubQueueCache(queuedCallback, callback);
        executor = Executors.newFixedThreadPool(10);
        tailHandler = handlerCaptor.getValue();
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
        simulateMessage(tailHandler, 200l);
        when(callback.fetchRange(Mockito.eq(100l), Mockito.eq(200l)))
                .thenReturn(Arrays.asList(createTestMessage(100, PUB_AGENT_NAME_1, ReqType.ADD)));
        Future<OffsetQueue<DistributionQueueItem>> consumer = consumer(PUB_AGENT_NAME_1, 100);
        // the consumer returns the offset queue
        consumer.get(15, SECONDS);
        assertEquals(100l, cache.getMinOffset());
    }

	@Test
    public void testFetchWithConcurrentConsumer() throws Exception {
	    simulateMessage(tailHandler, 200);
        // build two consumers for same agent queue, from offset 100
        Future<OffsetQueue<DistributionQueueItem>> consumer1 = consumer(PUB_AGENT_NAME_1, 100);
        Future<OffsetQueue<DistributionQueueItem>> consumer2 = consumer(PUB_AGENT_NAME_1, 100);
        when(callback.fetchRange(Mockito.eq(100l), Mockito.eq(200l)))
        .thenReturn(Arrays.asList(createTestMessage(100, PUB_AGENT_NAME_1, ReqType.ADD)));
        OffsetQueue<DistributionQueueItem> q1 = consumer1.get(5, SECONDS);
        OffsetQueue<DistributionQueueItem> q2 = consumer2.get(5, SECONDS);
        assertEquals(q1.getSize(), q2.getSize());
        assertEquals(100, cache.getMinOffset());
        
        // Fetch should only happen once
        verify(callback, times(1)).fetchRange(Mockito.anyLong(), Mockito.anyLong());
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

    private void simulateMessage(MessageHandler<PackageMessage> handler, long offset) {
        simulateMessage(handler,
                pickAny(PUB_AGENT_NAME_1, PUB_AGENT_NAME_2, PUB_AGENT_NAME_3),
                pickAny(ReqType.ADD, ReqType.DELETE, ReqType.TEST), offset);
    }

    private void simulateMessage(MessageHandler<PackageMessage> handler, String pubAgentName, ReqType reqType, long offset) {
        PackageMessage msg = createTestMessage(pubAgentName, reqType);
        simulateMessage(handler, msg, offset);
    }

    private void simulateMessage(MessageHandler<PackageMessage> handler, PackageMessage msg, long offset) {
        log.info("Simulate msg @ offset {}", offset);
        handler.handle(createInfo(offset), msg);
    }
    
    private FullMessage<PackageMessage> createTestMessage(long offset, String pubAgentName, ReqType reqType) {
        return new FullMessage<>(createInfo(offset), createTestMessage(pubAgentName, reqType));
    }

    private PackageMessage createTestMessage(String pubAgentName, ReqType reqType) {
        return PackageMessage.builder()
                .pkgType("pkgType")
                .pkgId(UUID.randomUUID().toString())
                .pubSlingId("pubSlingId")
                .reqType(reqType)
                .pubAgentName(pubAgentName)
                .build();
    }

    private MessageInfo createInfo(long offset) {
        return new TestMessageInfo(TOPIC, 0, offset, currentTimeMillis());
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

}