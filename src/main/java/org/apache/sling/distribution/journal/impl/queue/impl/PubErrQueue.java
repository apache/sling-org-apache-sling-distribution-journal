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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.DistributionQueueState;
import org.apache.sling.distribution.queue.DistributionQueueStatus;
import org.apache.sling.distribution.queue.DistributionQueueType;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

@ParametersAreNonnullByDefault
public class PubErrQueue implements DistributionQueue {

    private static final String UNSUPPORTED_CLEAR_OPERATION = "Unsupported clear operation";
    private static final Logger LOG = LoggerFactory.getLogger(PubErrQueue.class);

    private final OffsetQueue<DistributionQueueItem> agentQueue;

    private final  QueueEntryFactory entryFactory;

    private final OffsetQueue<Long> errorQueue;

    private final String queueName;

    public PubErrQueue(String queueName, OffsetQueue<DistributionQueueItem> agentQueue, OffsetQueue<Long> errorQueue) {
        this.queueName = requireNonNull(queueName);
        this.agentQueue = requireNonNull(agentQueue);
        this.errorQueue = requireNonNull(errorQueue);
        this.entryFactory = new QueueEntryFactory(queueName, queueItem -> 0);
    }

    @Nonnull
    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public DistributionQueueEntry add(@Nonnull DistributionQueueItem distributionQueueItem) {
        throw new UnsupportedOperationException("Unsupported add operation");
    }

    @Override
    public DistributionQueueEntry getHead() {
        Long refOffset = errorQueue.getHeadItem();
        if (refOffset != null) {
            DistributionQueueItem queueItem = agentQueue.getItem(refOffset);
            return entryFactory.create(queueItem);
        }
        return null;
    }

    @Nonnull
    @Override
    public Iterable<DistributionQueueEntry> getEntries(int skip, int limit) {
        List<DistributionQueueEntry> entries = new ArrayList<>();
        for (long refOffset : errorQueue.getHeadItems(skip, limit)) {
            DistributionQueueItem queueItem = agentQueue.getItem(refOffset);
            if (queueItem != null) {
                entries.add(entryFactory.create(queueItem));
            } else {
                LOG.warn("queueItem at offset {} not found", refOffset);
            }
        }
        return entries;
    }

    @Override
    public DistributionQueueEntry getEntry(@Nonnull String entryId) {
        DistributionQueueItem queueItem = agentQueue.getItem(EntryUtil.entryOffset(entryId));
        return (queueItem != null)
                ? entryFactory.create(queueItem)
                : null;
    }

    @Override
    public DistributionQueueEntry remove(@Nonnull String entryId) {
        throw new UnsupportedOperationException(UNSUPPORTED_CLEAR_OPERATION);
    }

    @Nonnull
    @Override
    public Iterable<DistributionQueueEntry> remove(Set<String> entryIds) {
        throw new UnsupportedOperationException(UNSUPPORTED_CLEAR_OPERATION);
    }

    @Nonnull
    @Override
    public Iterable<DistributionQueueEntry> clear(int limit) {
        throw new UnsupportedOperationException(UNSUPPORTED_CLEAR_OPERATION);
    }

    @Nonnull
    @Override
    public DistributionQueueStatus getStatus() {
        return new DistributionQueueStatus(errorQueue.getSize(), DistributionQueueState.PASSIVE);
    }

    @Nonnull
    @Override
    public DistributionQueueType getType() {
        return DistributionQueueType.ORDERED;
    }

    @Override
    public boolean hasCapability(String capability) {
        return false;
    }

}
