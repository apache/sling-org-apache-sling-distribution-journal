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

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;

@ParametersAreNonnullByDefault
public class OffsetQueueImpl<T> implements OffsetQueue<T>, OffsetQueueImplMBean {

    /**
     * Stores (offset x T) tuple ordered by offset (ascending).
     */
    private final SortedMap<Long, T> entries;

    public OffsetQueueImpl() {
        this(new ConcurrentSkipListMap<>());
    }
    
    private OffsetQueueImpl(SortedMap<Long, T> entries) {
        this.entries = Objects.requireNonNull(entries);
    }

    @Override
    public void putItem(long offset, T item) {
        entries.put(offset, item);
    }

    @Override
    public void putItems(OffsetQueue<T> offsetQueue) {
        if (offsetQueue instanceof OffsetQueueImpl) {
            entries.putAll(((OffsetQueueImpl<T>) offsetQueue).entries);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported offsetQueue type %s", offsetQueue));
        }
    }

    @Override
    public T getItem(long offset) {
        return entries.get(offset);
    }

    @Override
    public T getHeadItem() {
        try {
            long firstKey = entries.firstKey();
            return entries.get(firstKey);
        } catch (NoSuchElementException ignore) {
            // ignore
        }
        return null;
    }

    @Nonnull
    @Override
    public Iterable<T> getHeadItems(int skip, int limit) {
    	int maxSize = limit == -1 ? Integer.MAX_VALUE : limit;
        return entries.values().stream()
                .skip(skip)
                .limit(maxSize)
                .collect(Collectors.toList());
    }

    @Override
    public long getHeadOffset() {
        try {
            return entries.firstKey();
        } catch (NoSuchElementException ignore) {
            return -1;
        }
    }

    @Override
    public long getTailOffset() {
        try {
            return entries.lastKey();
        } catch (NoSuchElementException ignore) {
            return -1;
        }
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public int getSize() {
        return entries.size();
    }
    
    @Nonnull
    @Override
    public OffsetQueue<T> getMinOffsetQueue(long minOffset) {
        return new OffsetQueueImpl<>(entries.tailMap(minOffset));
    }
}
