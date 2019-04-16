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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.DistributionQueueType;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.junit.Test;

import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.*;
import static java.util.Collections.*;
import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class PubQueueTest {

    private static final String TOPIC = "topic";

    private static final String PARTITION = "0";

    private static final String QUEUE_NAME = "queueName";

    private static final String PACKAGE_ID_1 = "package-1";

    private static final String PACKAGE_ID_2 = "package-2";

    private static final String PACKAGE_ID_3 = "package-3";

    private static final ClearCallback NO_OP = (offset) -> {};

    private static final OffsetQueue<DistributionQueueItem> EMPTY_QUEUE = new OffsetQueueImpl<>();

    private static final OffsetQueue<DistributionQueueItem> THREE_ENTRY_QUEUE = new OffsetQueueImpl<>();

    static {

        THREE_ENTRY_QUEUE.putItem(100, new DistributionQueueItem(PACKAGE_ID_1, new HashMap<String, Object>(){{
            put(RECORD_TOPIC, TOPIC);
            put(RECORD_PARTITION, PARTITION);
            put(RECORD_OFFSET, 100);
            put(RECORD_TIMESTAMP, 1541538150582L);
        }}));

        THREE_ENTRY_QUEUE.putItem(200, new DistributionQueueItem(PACKAGE_ID_2, new HashMap<String, Object>(){{
            put(RECORD_TOPIC, TOPIC);
            put(RECORD_PARTITION, PARTITION);
            put(RECORD_OFFSET, 200);
            put(RECORD_TIMESTAMP, 1541538150584L);
        }}));

        THREE_ENTRY_QUEUE.putItem(300, new DistributionQueueItem(PACKAGE_ID_3, new HashMap<String, Object>(){{
            put(RECORD_TOPIC, TOPIC);
            put(RECORD_PARTITION, PARTITION);
            put(RECORD_OFFSET, 300);
            put(RECORD_TIMESTAMP, 1541538150586L);
        }}));
    }

    @Test
    public void testGetName() throws Exception {
        assertEquals(QUEUE_NAME, new PubQueue(QUEUE_NAME, new OffsetQueueImpl<>(), 0, NO_OP).getName());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAdd() throws Exception {
        DistributionQueueItem queueItem = new DistributionQueueItem(PACKAGE_ID_1, emptyMap());
        new PubQueue(QUEUE_NAME, new OffsetQueueImpl<>(), 0, NO_OP).add(queueItem);
    }

    @Test
    public void testGetHead() throws Exception {
        assertNull(pubQueue(EMPTY_QUEUE).getHead());
        DistributionQueueEntry headEntry = pubQueue(THREE_ENTRY_QUEUE).getHead();
        assertNotNull(headEntry);
        assertEquals(PACKAGE_ID_1, headEntry.getItem().getPackageId());
    }

    @Test
    public void testGetItems() throws Exception {
        Iterator<DistributionQueueEntry> entries = pubQueue(THREE_ENTRY_QUEUE).getEntries(1, 2).iterator();
        assertNotNull(entries);
        DistributionQueueEntry entry1 = entries.next();
        assertNotNull(entry1);
        assertEquals(PACKAGE_ID_2, entry1.getItem().getPackageId());
        DistributionQueueEntry entry2 = entries.next();
        assertEquals(PACKAGE_ID_3, entry2.getItem().getPackageId());
    }

    @Test
    public void testGetItem() throws Exception {
        String entryId = TOPIC + "-" + PARTITION + "@" + 200;
        DistributionQueueEntry queueEntry = pubQueue(THREE_ENTRY_QUEUE).getEntry(entryId);
        assertNotNull(queueEntry);
        assertEquals(PACKAGE_ID_2, queueEntry.getItem().getPackageId());
    }

    @Test
    public void testRemoveHead() throws Exception {
        final Semaphore invoked = new Semaphore(1);
        DistributionQueue queue = pubQueue(THREE_ENTRY_QUEUE, (offset) -> invoked.release());
        String headEntryId = EntryUtil.entryId(THREE_ENTRY_QUEUE.getHeadItem());
        DistributionQueueEntry removed = queue.remove(headEntryId);
        assertTrue(invoked.tryAcquire(5, TimeUnit.MILLISECONDS));
        assertEquals(headEntryId, removed.getId());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveRandomItemFails() throws Exception {
        DistributionQueue queue = pubQueue(THREE_ENTRY_QUEUE);
        String randomEntryId = EntryUtil.entryId(THREE_ENTRY_QUEUE.getItem(200));
        queue.remove(randomEntryId);
    }

    @Test
    public void testRemoveSetOfRandomItemsWillClear() throws Exception {
        final Semaphore invoked = new Semaphore(2);
        DistributionQueue queue = pubQueue(THREE_ENTRY_QUEUE, (offset) -> invoked.release());
        String headEntryId = EntryUtil.entryId(THREE_ENTRY_QUEUE.getHeadItem());
        String randomEntryId = EntryUtil.entryId(THREE_ENTRY_QUEUE.getItem(200));

        Iterator<DistributionQueueEntry> removed = queue.remove(Collections.singleton(randomEntryId)).iterator();
        assertTrue(invoked.tryAcquire(5, TimeUnit.MILLISECONDS));
        assertTrue(invoked.tryAcquire(5, TimeUnit.MILLISECONDS));
        assertEquals(headEntryId, removed.next().getId());
        assertEquals(randomEntryId, removed.next().getId());
        assertFalse(removed.hasNext());
    }

    @Test
    public void testRemoveSetOfNonExistingItem() throws Exception {
        DistributionQueue queue = pubQueue(THREE_ENTRY_QUEUE);

        Iterable<DistributionQueueEntry> removed = queue.remove(Collections.singleton("nonexisting-0@99999"));
        assertFalse(removed.iterator().hasNext());
    }

    @Test
    public void testClearAll() throws Exception {
        final Semaphore invoked = new Semaphore(3);
        DistributionQueue queue = pubQueue(THREE_ENTRY_QUEUE, (offset) -> invoked.release());

        Iterable<DistributionQueueEntry> removed = queue.clear(-1);
        assertTrue(invoked.tryAcquire(5, TimeUnit.MILLISECONDS));
        assertTrue(invoked.tryAcquire(5, TimeUnit.MILLISECONDS));
        assertTrue(invoked.tryAcquire(5, TimeUnit.MILLISECONDS));
        assertEquals(3, StreamSupport.stream(removed.spliterator(), false).count());
    }

    @Test
    public void testClearPartial() throws Exception {
        final Semaphore invoked = new Semaphore(2);
        DistributionQueue queue = pubQueue(THREE_ENTRY_QUEUE, (offset) -> invoked.release());

        Iterable<DistributionQueueEntry> removed = queue.clear(2);
        assertTrue(invoked.tryAcquire(5, TimeUnit.MILLISECONDS));
        assertTrue(invoked.tryAcquire(5, TimeUnit.MILLISECONDS));
        assertEquals(2, StreamSupport.stream(removed.spliterator(), false).count());
    }

    @Test
    public void testGetType() throws Exception {
        assertEquals(DistributionQueueType.ORDERED, new PubQueue(QUEUE_NAME, new OffsetQueueImpl<>(), 0, NO_OP).getType());
    }

    private PubQueue pubQueue(OffsetQueue<DistributionQueueItem> offsetQueue) {
        return pubQueue(offsetQueue, NO_OP);
    }

    private PubQueue pubQueue(OffsetQueue<DistributionQueueItem> offsetQueue, ClearCallback clearCallback) {
        return new PubQueue(QUEUE_NAME, offsetQueue, 0, clearCallback);
    }


}