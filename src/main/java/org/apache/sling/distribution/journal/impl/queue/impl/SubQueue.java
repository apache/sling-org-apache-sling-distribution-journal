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
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.apache.sling.distribution.journal.shared.PackageRetries;
import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.DistributionQueueItemState;
import org.apache.sling.distribution.queue.DistributionQueueState;
import org.apache.sling.distribution.queue.DistributionQueueStatus;
import org.apache.sling.distribution.queue.DistributionQueueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.distribution.queue.DistributionQueueItemState.QUEUED;
import static org.apache.sling.distribution.queue.DistributionQueueState.BLOCKED;
import static org.apache.sling.distribution.queue.DistributionQueueState.IDLE;
import static org.apache.sling.distribution.queue.DistributionQueueState.RUNNING;
import static org.apache.sling.distribution.queue.DistributionQueueType.ORDERED;

@ParametersAreNonnullByDefault
public class SubQueue implements DistributionQueue {

    private static final String UNSUPPORTED_CLEAR_OPERATION = "Unsupported clear operation";

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(SubQueue.class);

    private final DistributionQueueItem headItem;

    private final PackageRetries packageRetries;

    private final String queueName;

	private final  QueueEntryFactory entryFactory;

    public SubQueue(String queueName,
                    @Nullable
                    DistributionQueueItem headItem,
                    PackageRetries packageRetries) {
        this.headItem = headItem;
        this.queueName = Objects.requireNonNull(queueName);
        this.packageRetries = Objects.requireNonNull(packageRetries);
        this.entryFactory = new QueueEntryFactory(queueName, this::attempts);
    }

    @Nonnull
    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public DistributionQueueEntry add(DistributionQueueItem queueItem) {
        throw new UnsupportedOperationException("Unsupported add operation");
    }

    @Override
    @CheckForNull
    public DistributionQueueEntry getHead() {
        return entryFactory.create(headItem);
    }

    @Nonnull
    @Override
    public Iterable<DistributionQueueEntry> getEntries(int skip, int limit) {
        final List<DistributionQueueEntry> entries;
        if (skip == 0 && (limit == -1 || limit > 0) && headItem != null) {
            entries = Collections.singletonList(entryFactory.create(headItem));
        } else {
            entries = Collections.emptyList();
        }
        return Collections.unmodifiableList(entries);
    }

    @Override
    public DistributionQueueEntry getEntry(String entryId) {
        return (entryId.equals(EntryUtil.entryId(headItem)))
                ? entryFactory.create(headItem)
                : null;
    }

    @Override
    public DistributionQueueEntry remove(String entryId) {
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
        final DistributionQueueState queueState;
        final int itemsCount;
        DistributionQueueEntry headEntry = getHead();
        if (headEntry != null) {
            itemsCount = 1;
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

    @Override
    @Nonnull
    public DistributionQueueType getType() {
        return ORDERED;
    }

    @Override
    public boolean hasCapability(String capability) {
        return false;
    }

    private int attempts(DistributionQueueItem queueItem) {
        String entryId = EntryUtil.entryId(queueItem);
        return packageRetries.get(entryId);
    }

}
