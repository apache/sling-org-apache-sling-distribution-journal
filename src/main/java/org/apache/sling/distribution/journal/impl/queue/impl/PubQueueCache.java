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


import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static org.apache.sling.distribution.journal.HandlerAdapter.create;

import java.io.Closeable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.JMXRegistration;
import org.apache.sling.distribution.journal.shared.LocalStore;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.journal.impl.queue.QueueItemFactory;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;

/**
 * Cache the distribution packages fetched from the package topic.
 * The distribution packages associated to the request type are
 * handled by the cache but are not stored in the cache.
 *
 * The packages are fetched with two types of poller, the "tail"
 * and the "head" pollers.
 *
 * The "tail" poller keeps fetching the newest packages.
 * The "head" poller fetches the oldest packages on demand.
 */
@ParametersAreNonnullByDefault
public class PubQueueCache {

    private static final Logger LOG = LoggerFactory.getLogger(PubQueueCache.class);

    private static final long MAX_FETCH_WAIT_MS = 60 * 1000; // 1 minute

    /**
     * (pubAgentName x OffsetQueue)
     */
    private final Map<String, OffsetQueue<DistributionQueueItem>> agentQueues = new ConcurrentHashMap<>();

    /**
     * Only allows to fetch data from the journal
     * with a single thread.
     */
    private final Lock headPollerLock = new ReentrantLock();

    /**
     * Holds the min offset of package handled by the cache.
     * Given that the cache does not store all package messages
     * (i.e. TEST packages are not cached), the min offset does not
     * necessarily match the offset of the oldest message cached.
     */
    private final AtomicLong minOffset = new AtomicLong(Long.MAX_VALUE);

    /**
     * Holds the max offset of package handled by the cache.
     * Given that the cache does not store all package messages
     * (i.e. TEST packages are not cached), the max offset does not
     * necessarily match the offset of the newest message cached.
     */
    private final AtomicLong maxOffset = new AtomicLong(-1L);

    /**
     * Holds the last known seed offset stored to the
     * seed store.
     */
    private volatile long seedOffset = -1L;

    private final Set<JMXRegistration> jmxRegs = new HashSet<>();

    private final MessagingProvider messagingProvider;

    private final EventAdmin eventAdmin;

    private volatile Closeable tailPoller;

    private final QueueCacheSeeder seeder;

    private final String topic;

    private final LocalStore seedStore;

    private final DistributionMetricsService distributionMetricsService;
    
    public PubQueueCache(MessagingProvider messagingProvider, EventAdmin eventAdmin, DistributionMetricsService distributionMetricsService, String topic, LocalStore seedStore, QueueCacheSeeder seeder) {
        this.messagingProvider = messagingProvider;
        this.eventAdmin = eventAdmin;
        this.distributionMetricsService = distributionMetricsService;
        this.topic = topic;
        this.seedStore = seedStore;
        this.seeder = seeder;
        Long offset = seedStore.load("offset", Long.class);
        if (offset != null) {
            seedOffset = offset;
            startPoller(seedOffset);
            /*
             * We need at least one seeding message
             * for cases where the seedOffset is no
             * longer on the journal.
             */
            seeder.seedOne();
        } else {
            /*
             * Fallback to seeding messages when
             * no offset could be found in the
             * repository.
             */
            seeder.seed(this::startPoller);
        }
    }

    private void startPoller(long offset) {
        LOG.info("Seed with offset: {}", offset);
        String assignTo = messagingProvider.assignTo(offset);
        tailPoller = messagingProvider.createPoller(
                this.topic,
                Reset.earliest,
                assignTo,
                create(PackageMessage.class, this::handlePackage) 
                );
    }

    @Nonnull
    public OffsetQueue<DistributionQueueItem> getOffsetQueue(String pubAgentName, long minOffset) throws InterruptedException {
        if (!isSeeded()) {
            throw new RuntimeException("Gave up waiting for seeded cache");
        }
        fetchIfNeeded(minOffset);
        return agentQueues.getOrDefault(pubAgentName, new OffsetQueueImpl<>());
    }

    public void storeSeed() {
        long newSeed = maxOffset.longValue();
        if (newSeed > seedOffset) {
            storeSeed(newSeed);
            seedOffset = newSeed;
        }
    }

    private void storeSeed(long offset) {
        LOG.info("Store seed offset {}", offset);
        try {
            seedStore.store("offset", offset);
        } catch (PersistenceException e) {
            LOG.warn("Failed to persist seed offset", e);
        }
    }

    public int size() {
        return agentQueues.values().stream().mapToInt(OffsetQueue::getSize).sum();
    }

    public void close() {
        IOUtils.closeQuietly(tailPoller);
        IOUtils.closeQuietly(seeder);
        jmxRegs.forEach(IOUtils::closeQuietly);
    }

    /**
     * Fetch the package messages from the requested min offset,
     * up to the current cached min offset.
     *
     * @param requestedMinOffset the min offset to start fetching data from
     */
    private void fetchIfNeeded(long requestedMinOffset) throws InterruptedException {
        long cachedMinOffset = getMinOffset();
        if (requestedMinOffset < cachedMinOffset) {

            LOG.debug("Requested min offset {} smaller than cached min offset {}", requestedMinOffset, cachedMinOffset);

            // Fetching data from a topic is a costly
            // operation. In most cases, we expect the queues
            // to be roughly at the same state, and thus attempt
            // to fetch roughly the same data concurrently.
            // In order to minimize the cost, we limit to
            // running a single head poller at the same time.
            //
            // This implies that concurrent requests that require
            // a head poller will block until the head poller is
            // available. The headPollerLock guarantees to not
            // run head pollers concurrently.
            boolean locked = headPollerLock.tryLock(MAX_FETCH_WAIT_MS, MILLISECONDS);
            if (! locked) {
                throw new RuntimeException("Gave up fetching queue state");
            }
            try {

                // Once the headPollerLock has been acquired,
                // we check whether the data must still be
                // fetched. The data may have been fetched
                // while waiting on the lock.
                cachedMinOffset = getMinOffset();
                if (requestedMinOffset < cachedMinOffset) {

                    fetch(requestedMinOffset, cachedMinOffset);
                }
            } finally {
                headPollerLock.unlock();
            }
        }
    }

    /**
     * The missing data is fetched and merged in the
     * cache.
     */
    private void fetch(long requestedMinOffset, long cachedMinOffset) throws InterruptedException {
        distributionMetricsService.getQueueCacheFetchCount().increment();
        RangePoller headPoller = new RangePoller(messagingProvider,
                topic,
                requestedMinOffset,
                cachedMinOffset);
        merge(headPoller.fetchRange());
        updateMinOffset(requestedMinOffset);
    }

    private boolean isSeeded() {
        return getMinOffset() != Long.MAX_VALUE;
    }

    protected long getMinOffset() {
        return minOffset.longValue();
    }

    private void updateMinOffset(long offset) {
        // atomically compare and set minOffset
        minOffset.accumulateAndGet(offset, Math::min);
    }

    private void updateMaxOffset(long offset) {
        // atomically compare and set maxOffset
        maxOffset.accumulateAndGet(offset, Math::max);
    }

    private void merge(List<FullMessage<PackageMessage>> messages) {
        LOG.debug("Merging fetched offsets");
        messages.stream()
            .filter(this::isNotTestMessage)
            .collect(groupingBy(message -> message.getMessage().getPubAgentName()))
            .forEach(this::mergeByAgent);
        // update the minOffset AFTER all the messages
        // have been merged in order to avoid concurrent
        // consumers to potentially miss the non merged
        // queue items
        messages.stream().findFirst().ifPresent(message ->
            updateMinOffset(message.getInfo().getOffset()));
    }
    
    private void mergeByAgent(String pubAgentName, List<FullMessage<PackageMessage>> messages) {
        OffsetQueueImpl<DistributionQueueItem> msgs = new OffsetQueueImpl<>();
        messages
            .forEach(message -> msgs.putItem(message.getInfo().getOffset(), QueueItemFactory.fromPackage(message)));
        getOrCreateQueue(pubAgentName).putItems(msgs);
        messages.forEach(this::sendQueuedEvent);
    }

    private void sendQueuedEvent(FullMessage<PackageMessage> fMessage) {
        PackageMessage message = fMessage.getMessage();
        final Event queuedEvent = DistributionEvent.eventPackageQueued(message, message.getPubAgentName());
        eventAdmin.postEvent(queuedEvent);
    }

    private OffsetQueue<DistributionQueueItem> getOrCreateQueue(String pubAgentName) {
        // atomically create a new queue for
        // the publisher agent if needed
        return agentQueues.computeIfAbsent(pubAgentName,
                this::createQueue);
    }

    private boolean isNotTestMessage(FullMessage<PackageMessage> message) {
        return message.getMessage().getReqType() != PackageMessage.ReqType.TEST;
    }

    private OffsetQueue<DistributionQueueItem> createQueue(String pubAgentName) {
        OffsetQueue<DistributionQueueItem> agentQueue = new OffsetQueueImpl<>();
        jmxRegs.add(new JMXRegistration(agentQueue, OffsetQueue.class.getSimpleName(), pubAgentName));
        return agentQueue;
    }

    private void handlePackage(final MessageInfo info, final PackageMessage message) {
        merge(singletonList(new FullMessage<>(info, message)));
        updateMaxOffset(info.getOffset());
    }
}
