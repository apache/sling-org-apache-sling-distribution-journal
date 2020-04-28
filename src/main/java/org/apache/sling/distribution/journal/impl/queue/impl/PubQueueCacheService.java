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

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.impl.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.JournalAvailable;

import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;

@Component(
        service = {PubQueueCacheService.class, Runnable.class},
        property = {
                PROPERTY_SCHEDULER_CONCURRENT + ":Boolean=false",
                PROPERTY_SCHEDULER_PERIOD + ":Long=" + 12 * 60 * 60 // 12 hours
        })
@ParametersAreNonnullByDefault
public class PubQueueCacheService implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(PubQueueCacheService.class);

    /**
     * The minimum size to collect the cache. Each cache entry requires
     * around 500B of heap space. 10'000 entries ~= 5MB on heap.
     */
    private static final int CLEANUP_THRESHOLD = 10_000;

    /**
     * Interval in millisecond between two seeding messages to seed the cache.
     */
    private static final long CACHE_SEEDING_DELAY_MS = 10_000;

    /**
     * Will cause the cache to be cleared when we loose the journal
     */
    @Reference
    private JournalAvailable journalAvailable;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    @Reference
    private EventAdmin eventAdmin;

    @Reference
    private DistributionMetricsService distributionMetricsService;

    private volatile PubQueueCache cache;

    public PubQueueCacheService() {}

    public PubQueueCacheService(MessagingProvider messagingProvider,
                                Topics topics,
                                EventAdmin eventAdmin) {
        this.messagingProvider = messagingProvider;
        this.topics = topics;
        this.eventAdmin = eventAdmin;
    }

    @Activate
    public void activate() {
        cache = newCache();
        LOG.info("Started Publisher queue cache service");
    }

    @Deactivate
    public void deactivate() {
        if (cache != null) {
            cache.close();
        }
        LOG.info("Stopped Publisher queue cache service");
    }

    @Nonnull
    public OffsetQueue<DistributionQueueItem> getOffsetQueue(String pubAgentName, long minOffset) {
        try {
            return cache.getOffsetQueue(pubAgentName, minOffset);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void cleanup() {
        if (cache != null) {
            int size = cache.size();
            if (size > CLEANUP_THRESHOLD) {
                LOG.info("Cleanup package cache (size={}/{})", size, CLEANUP_THRESHOLD);
                cache.close();
                cache = newCache();
            } else {
                LOG.info("No cleanup required for package cache (size={}/{})", size, CLEANUP_THRESHOLD);
            }
        }
    }

    private PubQueueCache newCache() {
        return new PubQueueCache(messagingProvider, eventAdmin, distributionMetricsService, topics.getPackageTopic(), CACHE_SEEDING_DELAY_MS);
    }

    @Override
    public void run() {
        LOG.info("Starting package cache cleanup task");
        cleanup();
        LOG.info("Stopping package cache cleanup task");
    }
}
