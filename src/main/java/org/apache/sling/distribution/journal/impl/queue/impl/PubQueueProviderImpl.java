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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.MessageInfo;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * As reading all messages is an expensive operation this component is activated lazily only when requested by the Publisher.
 */
@Component
@ParametersAreNonnullByDefault
public class PubQueueProviderImpl implements PubQueueProvider {

    private static final Logger LOG = LoggerFactory.getLogger(PubQueueProviderImpl.class);

    /*
     * (pubAgentName#subAgentId x OffsetQueue)
     */
    private final Map<String, OffsetQueue<Long>> errorQueues = new ConcurrentHashMap<>();

    @Reference
    private PubQueueCacheService pubQueueCacheService;

    public PubQueueProviderImpl() {
    }
    
    public PubQueueProviderImpl(
            PubQueueCacheService pubQueueCacheService) {
        this.pubQueueCacheService = pubQueueCacheService;
    }

    @Activate
    public void activate() {
        LOG.info("Started Publisher queue provider service");
    }

    @Deactivate
    public void deactivate() {
        LOG.info("Stopped Publisher queue provider service");
    }

    @Nonnull
    @Override
    public DistributionQueue getQueue(QueueId queueId, long minOffset, int headRetries, ClearCallback clearCallback) {
        OffsetQueue<DistributionQueueItem> agentQueue = pubQueueCacheService.getOffsetQueue(queueId.getPubAgentName(), minOffset);
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
            agentQueue = pubQueueCacheService.getOffsetQueue(queueId.getPubAgentName(), minReferencedOffset);
        }

        return new PubErrQueue(queueId.getQueueName(), agentQueue, errorQueue);
    }

    public void handleStatus(MessageInfo info, PackageStatusMessage message) {
        if (message.getStatus() == Status.REMOVED_FAILED) {
            QueueId queueId = new QueueId(message.getPubAgentName(), message.getSubSlingId(), message.getSubAgentName(), "");
            String errorQueueKey = queueId.getErrorQueueKey();
            OffsetQueue<Long> errorQueue = errorQueues.computeIfAbsent(errorQueueKey, key -> new OffsetQueueImpl<>());
            errorQueue.putItem(info.getOffset(), message.getOffset());
        }
    }

}
