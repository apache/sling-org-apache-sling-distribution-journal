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

import java.util.HashMap;
import java.util.Map;

import org.apache.sling.distribution.journal.impl.queue.QueueItemFactory;
import org.apache.sling.distribution.journal.shared.PackageRetries;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SubQueueTest {

    @Test
    public void testGetName() throws Exception {
        String queueName = "someQueue";
        SubQueue queue = new SubQueue(queueName, null, new PackageRetries());
        Assert.assertEquals(queueName, queue.getName());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAdd() throws Exception {
        SubQueue queue = new SubQueue("someQueue", null, new PackageRetries());
        queue.add(buildQueueItem("package-1"));
    }

    @Test
    public void testGetHead() throws Exception {
        SubQueue emptyQueue = new SubQueue("emptyQueue", null, new PackageRetries());
        Assert.assertNull(emptyQueue.getHead());
        SubQueue oneQueue = new SubQueue("oneQueue", buildQueueItem("1"), new PackageRetries());
        Assert.assertNotNull(oneQueue.getHead());
    }

    @Test
    public void testGetItems() throws Exception {
        SubQueue oneQueue = new SubQueue("oneQueue", null, new PackageRetries());
        Assert.assertNotNull(oneQueue.getEntries(0, 10));
        SubQueue tenQueue = new SubQueue("tenQueue", buildQueueItem("1"), new PackageRetries());
        Assert.assertEquals(1, Lists.newArrayList(tenQueue.getEntries(0, 10)).size());
        Assert.assertEquals(1, Lists.newArrayList(tenQueue.getEntries(0, -1)).size());
        Assert.assertEquals(0, Lists.newArrayList(tenQueue.getEntries(1, 10)).size());
    }

    private DistributionQueueItem buildQueueItem(String packageId) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(QueueItemFactory.RECORD_TOPIC, "topic");
        properties.put(QueueItemFactory.RECORD_OFFSET, 0);
        properties.put(QueueItemFactory.RECORD_PARTITION, 0);
        properties.put(QueueItemFactory.RECORD_TIMESTAMP, System.currentTimeMillis());
        return new DistributionQueueItem(packageId, properties);
    }
}