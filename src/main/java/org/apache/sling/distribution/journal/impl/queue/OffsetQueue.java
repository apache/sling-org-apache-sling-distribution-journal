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
package org.apache.sling.distribution.journal.impl.queue;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * A queue of {@code T} items indexed by offsets.
 *
 * The items are ordered in the queue according to their offset.
 * The item with the smallest offset is the head of the queue.
 * The item with the largest offset is the tail of the queue.
 *
 * @param <T> the type of the items stored in the queue
 */
public interface OffsetQueue<T> {

    /**
     * Put an item in the queue at the given offset.
     *
     * @param offset the offset associated to the item
     * @param item the item to be stored at the given offset
     */
    void putItem(long offset, T item);

    /**
     * Put all the items from the #offsetQueue in this queue.
     *
     * @param offsetQueue the queue to be merged into this queue
     */
    void putItems(OffsetQueue<T> offsetQueue);

    /**
     * Return the item at the given #offset.
     *
     * @param offset the offset of the item to be returned
     * @return the item ; or {@code null} if no item could be found
     *         at the given offset
     */
    @CheckForNull
    T getItem(long offset);

    /**
     * Return the item with the smallest offset in the queue.
     *
     * @return the first item in the queue ; or
     *         {@code null} if the queue is empty
     */
    @CheckForNull
    T getHeadItem();

    /**
     * Return #limit items after skipping #skip items from the queue.
     *
     * @param skip the number of items to skip
     * @param limit the maximum number of items to return. -1 will return all items.
     * @return an iterable containing the items
     */
    @Nonnull
    Iterable<T> getHeadItems(int skip, int limit);

    /**
     * @return the smallest offset stored in the queue ; or
     *         {@code -1} if the queue is empty.
     */
    long getHeadOffset();

    /**
     * @return the largest offset stored in the queue ; or
     *         {@code -1} if the queue is empty.
     */
    long getTailOffset();

    /**
     * @return {@code true} if the queue is empty ;
     *         {@code false} otherwise
     */
    boolean isEmpty();

    /**
     * @return the number of items in the queue
     */
    int getSize();

    /**
     * Return an {@link OffsetQueue} offset queue which
     * contains all the items from the current queue at
     * an offset greater or equal to #minOffset.
     *
     * @param minOffset the minimal offset of the items
     *                  in the returned offset queue
     * @return the offset queue
     */
    @Nonnull
    OffsetQueue<T> getMinOffsetQueue(long minOffset);
}
