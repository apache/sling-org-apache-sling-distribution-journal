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

import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_OFFSET;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_PARTITION;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_TIMESTAMP;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_TOPIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.DistributionQueueType;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class PubQueueTest {
    private static final String TOPIC = "topic";
    private static final String PARTITION = "0";
    private static final String QUEUE_NAME = "queueName";
    private static final String PACKAGE_ID_PREFIX = "package-";
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Semaphore invoked = new Semaphore(0);
    private long lastClearOffset = 0L;
    private OffsetQueue<DistributionQueueItem> offsetQueue;
    private PubQueue queue;

    @Before
    public void before () {
        offsetQueue = new OffsetQueueImpl<>();
        queue = new PubQueue(QUEUE_NAME, offsetQueue, 0, this::clearCallback);
    }

    @Test
    public void testGetName() throws Exception {
        assertEquals(QUEUE_NAME, queue.getName());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAdd() throws Exception {
        queue.add(queueItem(1));
    }
    
    @Test
    public void testGetHeadEmpty() throws Exception {
        assertNull(queue.getHead());
    }

    @Test
    public void testGetHead() throws Exception {
        addEntries();
        
        DistributionQueueEntry headEntry = queue.getHead();

        assertNotNull(headEntry);
        assertEquals(packageId(1), headEntry.getItem().getPackageId());
    }

    @Test
    public void testGetItems() throws Exception {
        addEntries();
        
        Iterator<DistributionQueueEntry> entries = queue.getEntries(1, 2).iterator();
        
        assertNotNull(entries);
        DistributionQueueEntry entry1 = entries.next();
        assertNotNull(entry1);
        assertEquals(packageId(2), entry1.getItem().getPackageId());
        DistributionQueueEntry entry2 = entries.next();
        assertEquals(packageId(3), entry2.getItem().getPackageId());
    }

    @Test
    public void testGetItem() throws Exception {
        addEntries();
        
        String entryId = TOPIC + "-" + PARTITION + "@" + 200;
        DistributionQueueEntry queueEntry = queue.getEntry(entryId);
        
        assertNotNull(queueEntry);
        assertEquals(packageId(2), queueEntry.getItem().getPackageId());
    }

    @Test
    public void testRemoveHead() throws Exception {
        addEntries();
        
        String headEntryId = EntryUtil.entryId(offsetQueue.getHeadItem());
        DistributionQueueEntry removed = queue.remove(headEntryId);
        
        assertClearCallbackInvoked();
        assertEquals(headEntryId, removed.getId());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveRandomItemFails() throws Exception {
        addEntries();
        
        String randomEntryId = EntryUtil.entryId(offsetQueue.getItem(200));
        queue.remove(randomEntryId);
    }

    @Test
    public void testRemoveSetOfRandomItemsWillClear() throws Exception {
        addEntries();
        String headEntryId = EntryUtil.entryId(offsetQueue.getHeadItem());
        String randomEntryId = EntryUtil.entryId(offsetQueue.getItem(offset(2)));

        Iterator<DistributionQueueEntry> removed = queue.remove(Collections.singleton(randomEntryId)).iterator();
        
        assertClearCallbackInvoked();
        assertEquals(headEntryId, removed.next().getId());
        assertEquals(randomEntryId, removed.next().getId());
        assertFalse(removed.hasNext());
    }

    @Test
    public void testRemoveSetOfNonExistingItem() throws Exception {
        addEntries();
        
        Iterable<DistributionQueueEntry> removed = queue.remove(Collections.singleton("nonexisting-0@99999"));
        
        assertFalse(removed.iterator().hasNext());
    }

    @Test
    public void testClearAll() throws Exception {
        addEntries();

        Iterable<DistributionQueueEntry> removed = queue.clear(-1);
        
        assertClearCallbackInvoked();
        assertEquals(3, streamOf(removed).count());
        assertEquals(offset(3), lastClearOffset);
    }

    @Test
    public void testClearPartial() throws Exception {
        addEntries();
        
        Iterable<DistributionQueueEntry> removed = queue.clear(2);
        
        assertClearCallbackInvoked();
        assertEquals(2, streamOf(removed).count());
        assertEquals(offset(2), lastClearOffset);
    }

    @Test
    public void testGetType() throws Exception {
        assertEquals(DistributionQueueType.ORDERED, queue.getType());
    }

    private void assertClearCallbackInvoked() throws InterruptedException {
        assertTrue(invoked.tryAcquire(5, TimeUnit.MILLISECONDS));
    }

    private void addEntries() {
        offsetQueue.putItem(offset(1), queueItem(1));
        offsetQueue.putItem(offset(2), queueItem(2));
        offsetQueue.putItem(offset(3), queueItem(3));
    }

    private DistributionQueueItem queueItem(int nr) {
        HashMap<String, Object> data = new HashMap<String, Object>(){{
            put(RECORD_TOPIC, TOPIC);
            put(RECORD_PARTITION, PARTITION);
            put(RECORD_OFFSET, offset(nr));
            put(RECORD_TIMESTAMP, 1541538150580L + nr * 2);
        }};
        return new DistributionQueueItem(packageId(nr), data);
    }

    private long offset(int nr) {
        return nr * 100;
    }
    
    private static String packageId(int nr) {
        return PACKAGE_ID_PREFIX + new Integer(nr).toString();
    }

    private Stream<DistributionQueueEntry> streamOf(Iterable<DistributionQueueEntry> entries) {
        return StreamSupport.stream(entries.spliterator(), false);
    }

    private void clearCallback(long offset) {
        log.info("Clearcallback with offset {}", offset);
        lastClearOffset = offset; 
        invoked.release();
    }
}