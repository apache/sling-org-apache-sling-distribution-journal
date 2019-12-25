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

import org.apache.sling.distribution.journal.impl.queue.QueueItemFactory;
import org.apache.sling.distribution.queue.DistributionQueueItem;

public final class EntryUtil {
    
    private EntryUtil() {
    }

    public static long entryOffset(String entryId) {
        String[] chunks = entryId.split("@");
        if (chunks.length != 2) {
            throw new IllegalArgumentException(String.format("Unsupported entryId format %s", entryId));
        }
        return Long.parseLong(chunks[1]);
    }

    public static String entryId(DistributionQueueItem item) {
        return String.format("%s-%s@%s",
                item.get(QueueItemFactory.RECORD_TOPIC, String.class),
                item.get(QueueItemFactory.RECORD_PARTITION, Integer.class),
                item.get(QueueItemFactory.RECORD_OFFSET, Integer.class));
    }
}
