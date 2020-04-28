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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OffsetQueueImplTest {
    
    private OffsetQueue<Long> queue;

    @Before
    public void before() {
        queue = new OffsetQueueImpl<>();
    }
    
    @Test
    public void testPutGetItem() throws Exception {
        long offset = 100;
        long value = 42L;
        queue.putItem(offset, value);
        assertThat(queue.getItem(offset), equalTo(value));
    }

    @Test
    public void testGetHeadItem() throws Exception {
        Assert.assertNull(queue.getHeadItem());
        populateQueue();
        assertThat(queue.getHeadItem(), equalTo(100L));
    }

    @Test
    public void testGetHeadItems() throws Exception {
        Iterable<Long> items = queue.getHeadItems(1, 2);
        Assert.assertFalse(items.iterator().hasNext());
        populateQueue();
        Iterator<Long> iterator = queue.getHeadItems(1, 2).iterator();
        assertThat(iterator.next(), equalTo(105L));
        assertThat(iterator.next(), equalTo(113L));
        Assert.assertFalse(iterator.hasNext());
        
        Iterator<Long> itemsAll = queue.getHeadItems(1, -1).iterator();
        assertAllItemsPresent(itemsAll);
    }

    @Test
    public void testGetHeadItemsOutOfBounds() throws Exception {
        populateQueue();
        Iterator<Long> iterator = queue.getHeadItems(100, 2).iterator();
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetHeadItemsNotEnoughItems() throws Exception {
        populateQueue();
        Iterator<Long> iterator = queue.getHeadItems(1, 200).iterator();
        assertAllItemsPresent(iterator);
    }

	private void assertAllItemsPresent(Iterator<Long> iterator) {
		Assert.assertTrue(iterator.hasNext());
        assertThat(iterator.next(), equalTo(105L));
        assertThat(iterator.next(), equalTo(113L));
        assertThat(iterator.next(), equalTo(155L));
        assertThat(iterator.next(), equalTo(194L));
        Assert.assertFalse(iterator.hasNext());
	}

    @Test
    public void testIsEmpty() throws Exception {
        Assert.assertTrue(queue.isEmpty());
        queue.putItem(100, 42L);
        Assert.assertFalse(queue.isEmpty());
    }

    @Test
    public void testSize() throws Exception {
        Assert.assertEquals(0, queue.getSize());
        queue.putItem(100, 42L);
        Assert.assertEquals(1, queue.getSize());
        queue.putItem(101, 42L);
        Assert.assertEquals(2, queue.getSize());
    }

    @Test
    public void testGetMinOffsetQueue() throws Exception {
        populateQueue();
        OffsetQueue<Long> minQueue = queue.getMinOffsetQueue(105);
        Assert.assertEquals(4, minQueue.getSize());
        long minValue = minQueue.getHeadItem();
        Assert.assertEquals(105, minValue);
    }

    @Test
    public void testGetMinOffsetQueueApproxOffset() throws Exception {
        populateQueue();
        OffsetQueue<Long> minQueue = queue.getMinOffsetQueue(103);
        Assert.assertEquals(4, minQueue.getSize());
        long minValue = minQueue.getHeadItem();
        Assert.assertEquals(105, minValue);
    }

    @Test
    public void testGetMinOffsetQueueEmptyQueue() throws Exception {
        OffsetQueue<Long> minQueue = queue.getMinOffsetQueue(103);
        Assert.assertEquals(0, minQueue.getSize());
    }

    @Test
    public void testGetMinOffsetQueueOutOfBoundOffset() throws Exception {
        populateQueue();
        OffsetQueue<Long> minQueue = queue.getMinOffsetQueue(200);
        Assert.assertEquals(0, minQueue.getSize());
    }

    private void populateQueue() {
        for (long offset : Arrays.asList(100, 105, 113, 155, 194)) {
            queue.putItem(offset, offset);
        }
    }

}