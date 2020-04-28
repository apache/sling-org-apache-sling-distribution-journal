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

import static java.util.Collections.emptyList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.sling.distribution.queue.DistributionQueueCapabilities.CLEARABLE;
import static org.apache.sling.distribution.queue.DistributionQueueCapabilities.REMOVABLE;
import static org.apache.sling.distribution.queue.DistributionQueueItemState.QUEUED;
import static org.apache.sling.distribution.queue.DistributionQueueState.BLOCKED;
import static org.apache.sling.distribution.queue.DistributionQueueState.IDLE;
import static org.apache.sling.distribution.queue.DistributionQueueState.RUNNING;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.DistributionQueueItemState;
import org.apache.sling.distribution.queue.DistributionQueueState;
import org.apache.sling.distribution.queue.DistributionQueueStatus;
import org.apache.sling.distribution.queue.DistributionQueueType;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ParametersAreNonnullByDefault
public class PubQueue implements DistributionQueue {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String queueName;

    private final Set<String> capabilities = new HashSet<>();

    private final OffsetQueue<DistributionQueueItem> offsetQueue;

    private final int retries;

    private final DistributionQueueItem headItem;

    private final ClearCallback clearCallback;

	private final QueueEntryFactory entryFactory;

    public PubQueue(String queueName,
                    OffsetQueue<DistributionQueueItem> offsetQueue,
                    int retries,
                    @Nullable ClearCallback clearCallback) {
        this.queueName = Objects.requireNonNull(queueName);
        this.offsetQueue = Objects.requireNonNull(offsetQueue);
        this.retries = retries;
        this.clearCallback = clearCallback;
        if (clearCallback != null) {
            capabilities.add(CLEARABLE);
            /*
             * The REMOVABLE capability is not meant to be
             * supported by the PubQueue. however we currently
             * claim to support it, because the UI is not yet
             * able to allow selecting a range for clearing the
             * queue.
             */
            capabilities.add(REMOVABLE);
        }

        this.entryFactory = new QueueEntryFactory(queueName, this::attempts);
        this.headItem = offsetQueue.getHeadItem();
    }
    
    @Nonnull
    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public DistributionQueueEntry add(DistributionQueueItem queueItem) {
        // The items are added to the queue by reading the package topic
        throw new UnsupportedOperationException("Unsupported add operation");
    }

    @Override
    public DistributionQueueEntry getHead() {
        DistributionQueueItem queueItem = offsetQueue.getHeadItem();
        return entryFactory.create(queueItem);
    }

    @Nonnull
    @Override
    public Iterable<DistributionQueueEntry> getEntries(int skip, int limit) {
        List<DistributionQueueEntry> entries = new ArrayList<>();
        for (DistributionQueueItem queueItem : offsetQueue.getHeadItems(skip, limit)) {
            entries.add(entryFactory.create(queueItem));
        }
        return entries;
    }

    @Override
    public DistributionQueueEntry getEntry(String entryId) {
        DistributionQueueItem queueItem = offsetQueue.getItem(EntryUtil.entryOffset(entryId));
        return entryFactory.create(queueItem);
    }

    @Override
    public DistributionQueueEntry remove(String entryId) {
        /*
         * When the UI is adapted to allow clearing a
         * range of items from the head, this method
         * should throw an UnsupportedOperationException
         * and the REMOVABLE capability must be removed.
         *
         * Until then, the REMOVABLE capability is provided
         * but only allows to remove the head of the queue.
         */
        log.info("Removing queue entry {}", entryId);
        DistributionQueueEntry headEntry = getHead();
        if (headEntry != null) {
            if (headEntry.getId().equals(entryId)) {
                clear(headEntry);
            } else {
                throw new UnsupportedOperationException("Unsupported random clear operation");
            }
        }
        return headEntry;
    }

    @Nonnull
    @Override
    public Iterable<DistributionQueueEntry> remove(Set<String> entryIds) {
        /*
         * When the UI is adapted to allow clearing a
         * range of items from the head, this method
         * should throw an UnsupportedOperationException
         * and the REMOVABLE capability must be removed.
         *
         * Until then, the REMOVABLE capability is provided
         * but is implemented as a CLEARABLE capability
         * which clears from the head entry to the entry
         * provided with the max offset (tailEntry).
         */
        log.info("Removing queue entries {}", entryIds);
        Optional<String> tailEntryId = entryIds.stream()
                .max(Comparator.comparingLong(EntryUtil::entryOffset));
        return tailEntryId.map(this::clear).orElse(emptyList());
    }

    @Nonnull
    @Override
    public Iterable<DistributionQueueEntry> clear(int limit) {
        Iterable<DistributionQueueEntry> removed = getEntries(0, limit);
        // find the tail removed entry and clear from it
        stream(removed.spliterator(), false)
                .reduce((e1, e2) -> e2)
                .ifPresent(this::clear);
        return removed;
    }

    @Nonnull
    @Override
    public DistributionQueueStatus getStatus() {
        final DistributionQueueState queueState;
        final int itemsCount;
        DistributionQueueEntry headEntry = getHead();
        if (headEntry != null) {
            itemsCount = offsetQueue.getSize();
            DistributionQueueItemState itemState = headEntry.getStatus().getItemState();
            if (itemState == QUEUED) {
                queueState = RUNNING;
            } else {
                queueState = BLOCKED;
            }
        } else {
            itemsCount = 0;
            queueState = IDLE;
        }
        return new DistributionQueueStatus(itemsCount, queueState);
    }

    @Nonnull
    @Override
    public DistributionQueueType getType() {
        return DistributionQueueType.ORDERED;
    }

    @Override
    public boolean hasCapability(String capability) {
        return capabilities.contains(capability);
    }

    private int attempts(DistributionQueueItem queueItem) {
        return queueItem.equals(headItem) ? retries : 0;
    }

    private Iterable<DistributionQueueEntry> clear(String tailEntryId) {
        log.info("Clearing up to tail queue entry {}", tailEntryId);
        List<DistributionQueueEntry> removed = new ArrayList<>();
        for (DistributionQueueEntry entry : getEntries(0, -1)) {
            removed.add(entry);
            if (tailEntryId.equals(entry.getId())) {
                clear(entry);
                return removed;
            }
        }
        return emptyList();
    }

    private void clear(DistributionQueueEntry tailEntry) {
        if (clearCallback == null) {
            throw new UnsupportedOperationException();
        }
        long offset = EntryUtil.entryOffset(tailEntry.getId());
        clearCallback.clear(offset);
    }

}
