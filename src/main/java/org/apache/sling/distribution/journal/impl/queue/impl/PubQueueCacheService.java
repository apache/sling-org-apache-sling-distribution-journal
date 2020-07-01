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

import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.LocalStore;
import org.apache.sling.distribution.journal.shared.PublisherConfigurationAvailable;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.settings.SlingSettingsService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(immediate = true, service = PubQueueCacheService.class)
@ParametersAreNonnullByDefault
public class PubQueueCacheService {

    private static final Logger LOG = LoggerFactory.getLogger(PubQueueCacheService.class);

    /**
     * The minimum size to collect the cache. Each cache entry requires
     * around 500B of heap space. 10'000 entries ~= 5MB on heap.
     */
    private static final int CLEANUP_THRESHOLD = 10_000;

    /**
     * Will cause the cache to be cleared when we loose the journal
     */
    @Reference
    private JournalAvailable journalAvailable;

    /**
     * The cache is active only when at least one DistributionSubscriber agent is configured.
     */
    @Reference
    private PublisherConfigurationAvailable publisherConfigurationAvailable;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    @Reference
    private EventAdmin eventAdmin;

    @Reference
    private DistributionMetricsService distributionMetricsService;

    @Reference
    private SlingSettingsService slingSettings;

    @Reference
    private ResourceResolverFactory resolverFactory;

    private volatile PubQueueCache cache;

    private String pubSlingId;

    public PubQueueCacheService() {}

    public PubQueueCacheService(MessagingProvider messagingProvider,
                                Topics topics,
                                EventAdmin eventAdmin,
                                SlingSettingsService slingSettingsService,
                                ResourceResolverFactory resolverFactory,
                                String pubSlingId) {
        this.messagingProvider = messagingProvider;
        this.topics = topics;
        this.eventAdmin = eventAdmin;
        this.slingSettings = slingSettingsService;
        this.resolverFactory = resolverFactory;
        this.pubSlingId = pubSlingId;
    }

    @Activate
    public void activate() {
        pubSlingId = slingSettings.getSlingId();
        cache = newCache();
        LOG.info("Started Publisher queue cache service");
    }

    @Deactivate
    public void deactivate() {
        PubQueueCache queueCache = this.cache;
        if (queueCache != null) {
            queueCache.close();
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

    /**
     * The cleanup renew the cache when
     * a capacity threshold has been reached.
     */
    public void cleanup() {
        PubQueueCache queueCache = this.cache;
        if (queueCache != null) {
            int size = queueCache.size();
            if (size > CLEANUP_THRESHOLD) {
                LOG.info("Cleanup package cache (size={}/{})", size, CLEANUP_THRESHOLD);
                queueCache.close();
                cache = newCache();
            } else {
                LOG.info("No cleanup required for package cache (size={}/{})", size, CLEANUP_THRESHOLD);
            }
        }
    }

    public void storeSeed() {
        PubQueueCache queueCache = this.cache;
        if (queueCache != null) {
            queueCache.storeSeed();
        }
    }

    private PubQueueCache newCache() {
        LocalStore seedStore = new LocalStore(resolverFactory, "seeds", pubSlingId);
        String topic = topics.getPackageTopic();
        QueueCacheSeeder seeder = new QueueCacheSeeder(messagingProvider, topic);
        return new PubQueueCache(messagingProvider, eventAdmin, distributionMetricsService, topic, seedStore, seeder);
    }
}
