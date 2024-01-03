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
package org.apache.sling.distribution.journal.queue.impl;

import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;

import java.util.Dictionary;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.impl.publisher.PackageQueuedNotifier;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.queue.CacheCallback;
import org.apache.sling.distribution.journal.queue.OffsetQueue;
import org.apache.sling.distribution.journal.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.queue.QueueState;
import org.apache.sling.distribution.journal.shared.AgentId;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ParametersAreNonnullByDefault
public class PubQueueProviderImpl implements PubQueueProvider, Runnable {
    /**
     * The minimum size to collect the cache. Each cache entry requires
     * around 500B of heap space. 10'000 entries ~= 5MB on heap.
     */
    private static final int CLEANUP_THRESHOLD = 10_000;

    private static final Logger LOG = LoggerFactory.getLogger(PubQueueProviderImpl.class);
    
    private final PackageQueuedNotifier queuedNotifier;

    private final CacheCallback callback;
    
    private volatile PubQueueCache cache; //NOSONAR

    private final QueueErrors queueErrors;

    /*
     * (pubAgentName#subAgentId x OffsetQueue)
     */
    private final Map<String, OffsetQueue<Long>> errorQueues = new ConcurrentHashMap<>();

    private ServiceRegistration<?> reg;

    public PubQueueProviderImpl(EventAdmin eventAdmin, QueueErrors queueErrors, CacheCallback callback, BundleContext context) {
        queuedNotifier = new PackageQueuedNotifier(eventAdmin);
        this.queueErrors = queueErrors;
        this.callback = callback;
        cache = newCache();
        startCleanupTask(context);
        LOG.info("Started Publisher queue provider service");
    }
    
    private void startCleanupTask(BundleContext context) {
        // Register periodic task to update the topology view
        Dictionary<String, Object> props = new Hashtable<>();
        props.put(PROPERTY_SCHEDULER_CONCURRENT, false);
        props.put(PROPERTY_SCHEDULER_PERIOD, 12*60*60L); // every 12 h
        reg = context.registerService(Runnable.class.getName(), this, props);
    }

    @Override
    public void close() {
        PubQueueCache queueCache = this.cache;
        if (queueCache != null) {
            queueCache.close();
        }
        if (reg != null) {
            try {
                reg.unregister();
                reg = null;
            } catch (Exception e) {
                LOG.info(e.getMessage(), e);
            }
        }
        IOUtils.closeQuietly(queuedNotifier);
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
                this.cache = newCache();
            } else {
                LOG.info("No cleanup required for package cache (size={}/{})", size, CLEANUP_THRESHOLD);
            }
        }
        LOG.info("Stopping package cache cleanup task");
    }
    
    @Nonnull
    @Override
    public Set<String> getQueueNames(String pubAgentName) {
        // Queues names are generated only for the subscriber agents which are
        // alive and are subscribed to the publisher agent name (pubAgentName).
        // The queue names match the subscriber agent identifier (subAgentId).
        //
        // If errors queues are enabled, an error queue name is generated which
        // follows the pattern "%s-error". The pattern is deliberately different
        // from the SCD on Jobs one ("error-%s") as we don't want to support
        // the UI ability to retry items from the error queue.
        Set<String> queueNames = new HashSet<>();
        for (String subAgentId : callback.getSubscribedAgentIds(pubAgentName)) {
            queueNames.add(subAgentId);
            QueueState subState = callback.getQueueState(pubAgentName, subAgentId);
            if (subState != null) {
                boolean errorQueueEnabled = (subState.getMaxRetries() >= 0);
                if (errorQueueEnabled) {
                    queueNames.add(String.format("%s-error", subAgentId));
                }
            }
        }
        return queueNames;
    }

    @Nonnull
    @Override
    public PackageQueuedNotifier getQueuedNotifier() {
        return queuedNotifier;
    }

    @Nullable
    @Override
    public DistributionQueue getQueue(String pubAgentName, String queueName) {
        if (queueName.endsWith("-error")) {
            return getErrorQueue(pubAgentName, queueName);
        } else {
            QueueState state = callback.getQueueState(pubAgentName, queueName);
            if (state == null) {
                return null;
            }
            long minOffset = state.getLastProcessedOffset() + 1; // Start from offset after last processed
            OffsetQueue<DistributionQueueItem> agentQueue = getOffsetQueue(pubAgentName, minOffset);
            Throwable error = queueErrors.getError(pubAgentName, queueName);
            return new PubQueue(queueName, agentQueue.getMinOffsetQueue(minOffset), state.getHeadRetries(), error, state.getClearCallback());
        }
    }

    @Nonnull
    private DistributionQueue getErrorQueue(String pubAgentName, String queueName) {
        AgentId subAgentId = new AgentId(StringUtils.substringBeforeLast(queueName, "-error"));
        String errorQueueKey = getErrorQueueKey(pubAgentName, subAgentId.getSlingId(), subAgentId.getAgentName());
        OffsetQueue<Long> errorQueue = errorQueues.getOrDefault(errorQueueKey, new OffsetQueueImpl<>());
        final Long minReferencedOffset = errorQueue.getHeadItem();
        final OffsetQueue<DistributionQueueItem> agentQueue;
        if (minReferencedOffset == null) {
            agentQueue = new OffsetQueueImpl<>();
        } else {
            agentQueue = getOffsetQueue(pubAgentName, minReferencedOffset);
        }

        return new PubErrQueue(queueName, agentQueue, errorQueue);
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
    

    public int getMaxQueueSize(String pubAgentName) {
        Optional<Long> minOffset = getMinOffset(pubAgentName);
        if (minOffset.isPresent()) {
            return getOffsetQueue(pubAgentName, minOffset.get()).getMinOffsetQueue(minOffset.get()).getSize();
        } else {
            return 0;
        }
    }

    private Optional<Long> getMinOffset(String pubAgentName) {
        return callback.getSubscribedAgentIds(pubAgentName).stream()
            .map(subAgentName -> lastProcessedOffset(pubAgentName, subAgentName))
            .min(Long::compare);
    }

    private long lastProcessedOffset(String pubAgentName, String subAgentName) {
        return callback.getQueueState(pubAgentName, subAgentName).getLastProcessedOffset();
    }

    public void handleStatus(MessageInfo info, PackageStatusMessage message) {
        if (message.getStatus() == Status.REMOVED_FAILED) {
            String errorQueueKey = getErrorQueueKey(message.getPubAgentName(), message.getSubSlingId(), message.getSubAgentName());
            OffsetQueue<Long> errorQueue = errorQueues.computeIfAbsent(errorQueueKey, key -> new OffsetQueueImpl<>());
            errorQueue.putItem(info.getOffset(), message.getOffset());
        }
    }
    
    private String getErrorQueueKey(String pubAgentName, String subSlingId, String subAgentName) {
        return String.format("%s#%s#%s", pubAgentName, subSlingId, subAgentName);
    }

    private PubQueueCache newCache() {
        return new PubQueueCache(queuedNotifier, callback);
    }

}
