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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.junit.Test;

public class EntryUtilTest {

    @SuppressWarnings("serial")
	@Test
    public void testEntryOffset() throws Exception {
        DistributionQueueItem queueItem = new DistributionQueueItem("packageId", new HashMap<String, Object>(){{
            put("recordTopic", "topic");
            put("recordPartition", 0);
            put("recordOffset", 100);
        }});
        assertEquals("topic-0@100", EntryUtil.entryId(queueItem));
    }

    @Test
    public void testEntryId() throws Exception {
        assertEquals(100, EntryUtil.entryOffset("topic-0@100"));
        assertEquals(0, EntryUtil.entryOffset("topic-0@0"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalEntryId() throws Exception {
        EntryUtil.entryOffset("illegal-0");
    }

}