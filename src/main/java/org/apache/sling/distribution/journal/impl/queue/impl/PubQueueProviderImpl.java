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

import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.impl.queue.CacheCallback;
import org.apache.sling.distribution.journal.impl.queue.ClearCallback;
import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.journal.impl.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.impl.queue.QueueId;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
        property = {
                PROPERTY_SCHEDULER_CONCURRENT + ":Boolean=false",
                PROPERTY_SCHEDULER_PERIOD + ":Long=" + 12 * 60 * 60 // 12 hours
        }
)
@ParametersAreNonnullByDefault
public class PubQueueProviderImpl implements PubQueueProvider, Runnable {
    /**
     * The minimum size to collect the cache. Each cache entry requires
     * around 500B of heap space. 10'000 entries ~= 5MB on heap.
     */
    private static final int CLEANUP_THRESHOLD = 10_000;

    private static final Logger LOG = LoggerFactory.getLogger(PubQueueProviderImpl.class);
    
    @Reference
    private EventAdmin eventAdmin;

    @Reference
    private CacheCallback callback;
    
    private volatile PubQueueCache cache; //NOSONAR

    /*
     * (pubAgentName#subAgentId x OffsetQueue)
     */
    private final Map<String, OffsetQueue<Long>> errorQueues = new ConcurrentHashMap<>();

    public PubQueueProviderImpl() {
    }
    
    public PubQueueProviderImpl(EventAdmin eventAdmin, CacheCallback callback) {
        this.eventAdmin = eventAdmin;
        this.callback = callback;
    }

    @Activate
    public void activate() {
        cache = newCache();
        LOG.info("Started Publisher queue provider service");
    }

    @Deactivate
    public void deactivate() {
        PubQueueCache queueCache = this.cache;
        if (queueCache != null) {
            queueCache.close();
        }
        LOG.info("Stopped Publisher queue provider service");
    }
    
    @Override
    public void run() {
        LOG.info("Starting package cache cleanup task");
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
        LOG.info("Stopping package cache cleanup task");
    }

    @Nonnull
    @Override
    public DistributionQueue getQueue(QueueId queueId, long minOffset, int headRetries, ClearCallback clearCallback) {
        OffsetQueue<DistributionQueueItem> agentQueue = getOffsetQueue(queueId.getPubAgentName(), minOffset);
        return new PubQueue(queueId.getQueueName(), agentQueue.getMinOffsetQueue(minOffset), headRetries, clearCallback);
    }

    @Nonnull
    @Override
    public DistributionQueue getErrorQueue(QueueId queueId) {
        String errorQueueKey = queueId.getErrorQueueKey();
        OffsetQueue<Long> errorQueue = errorQueues.getOrDefault(errorQueueKey, new OffsetQueueImpl<>());
        long headOffset = errorQueue.getHeadOffset();
        final OffsetQueue<DistributionQueueItem> agentQueue;
        if (headOffset < 0) {
            agentQueue = new OffsetQueueImpl<>();
        } else {
            long minReferencedOffset = errorQueue.getItem(headOffset);
            agentQueue = getOffsetQueue(queueId.getPubAgentName(), minReferencedOffset);
        }

        return new PubErrQueue(queueId.getQueueName(), agentQueue, errorQueue);
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

    public void handleStatus(MessageInfo info, PackageStatusMessage message) {
        if (message.getStatus() == Status.REMOVED_FAILED) {
            QueueId queueId = new QueueId(message.getPubAgentName(), message.getSubSlingId(), message.getSubAgentName(), "");
            String errorQueueKey = queueId.getErrorQueueKey();
            OffsetQueue<Long> errorQueue = errorQueues.computeIfAbsent(errorQueueKey, key -> new OffsetQueueImpl<>());
            errorQueue.putItem(info.getOffset(), message.getOffset());
        }
    }

    private PubQueueCache newCache() {
        return new PubQueueCache(eventAdmin, callback);
    }

}
