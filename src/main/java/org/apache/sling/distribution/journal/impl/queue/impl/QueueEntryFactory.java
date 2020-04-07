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

import static org.apache.sling.distribution.queue.DistributionQueueItemState.ERROR;
import static org.apache.sling.distribution.queue.DistributionQueueItemState.QUEUED;

import java.util.Calendar;
import java.util.function.ToIntFunction;

import org.apache.sling.distribution.journal.impl.queue.QueueItemFactory;
import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.DistributionQueueItemState;
import org.apache.sling.distribution.queue.DistributionQueueItemStatus;

public class QueueEntryFactory {

    private final String queueName;
    private final ToIntFunction<DistributionQueueItem> attemptsCallback;

    public QueueEntryFactory(String queueName, ToIntFunction<DistributionQueueItem> attemptsCallback) {
        this.queueName = queueName;
        this.attemptsCallback = attemptsCallback;
    }

    public DistributionQueueEntry create(DistributionQueueItem queueItem) {
        if (queueItem == null) {
            return null;
        }
        String entryId = EntryUtil.entryId(queueItem);
        DistributionQueueItemStatus itemStatus = buildQueueItemStatus(queueItem);
        return new DistributionQueueEntry(entryId, queueItem, itemStatus);
    }

    private DistributionQueueItemStatus buildQueueItemStatus(DistributionQueueItem queueItem) {
        int attempts = attemptsCallback.applyAsInt(queueItem);
        DistributionQueueItemState state = (attempts > 0) ? ERROR : QUEUED;
        return new DistributionQueueItemStatus(itemCalendar(queueItem), state, attempts, queueName);
    }

    private Calendar itemCalendar(DistributionQueueItem queueItem) {
        long recordTimestamp = queueItem.get(QueueItemFactory.RECORD_TIMESTAMP, Long.class);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(recordTimestamp);
        return calendar;
    }
}
