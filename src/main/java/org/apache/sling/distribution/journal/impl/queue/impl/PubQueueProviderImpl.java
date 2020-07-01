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

import static org.apache.sling.distribution.journal.HandlerAdapter.create;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.journal.impl.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.messages.ClearCommand;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.shared.Topics;
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
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    @Reference
    private PubQueueCacheService pubQueueCacheService;

    private Closeable statusPoller;

    private Consumer<ClearCommand> sender;

    public PubQueueProviderImpl() {
    }
    
    public PubQueueProviderImpl(
            PubQueueCacheService pubQueueCacheService,
            MessagingProvider messagingProvider,
            Topics topics) {
        this.pubQueueCacheService = pubQueueCacheService;
        this.messagingProvider = messagingProvider;
        this.topics = topics;
    }

    @Activate
    public void activate() {
        statusPoller = messagingProvider.createPoller(
                topics.getStatusTopic(),
                Reset.earliest,
                create(PackageStatusMessage.class, this::handleStatus)
                );
        sender = messagingProvider.createSender(topics.getCommandTopic());
        LOG.info("Started Publisher queue provider service");
    }

    @Deactivate
    public void deactivate() {
        IOUtils.closeQuietly(statusPoller);
        LOG.info("Stopped Publisher queue provider service");
    }

    @Nonnull
    @Override
    public DistributionQueue getQueue(String pubAgentName, String subSlingId, String subAgentName, String queueName, long minOffset, int headRetries, boolean editable) {
        OffsetQueue<DistributionQueueItem> agentQueue = pubQueueCacheService.getOffsetQueue(pubAgentName, minOffset);
        ClearCallback editableCallback = offset -> sendClearCommand(subSlingId, subAgentName, offset);
        ClearCallback callback = editable ? editableCallback : null;
        return new PubQueue(queueName, agentQueue.getMinOffsetQueue(minOffset), headRetries, callback);
    }

    @Nonnull
    @Override
    public DistributionQueue getErrorQueue(String pubAgentName, String subSlingId, String subAgentName, String queueName) {
        String errorQueueKey = errorQueueKey(pubAgentName, subSlingId, subAgentName);
        OffsetQueue<Long> errorQueue = errorQueues.getOrDefault(errorQueueKey, new OffsetQueueImpl<>());
        long headOffset = errorQueue.getHeadOffset();
        final OffsetQueue<DistributionQueueItem> agentQueue;
        if (headOffset < 0) {
            agentQueue = new OffsetQueueImpl<>();
        } else {
            long minReferencedOffset = errorQueue.getItem(headOffset);
            agentQueue = pubQueueCacheService.getOffsetQueue(pubAgentName, minReferencedOffset);
        }

        return new PubErrQueue(queueName, agentQueue, errorQueue);
    }

    public void handleStatus(MessageInfo info, PackageStatusMessage message) {
        if (message.getStatus() == Status.REMOVED_FAILED) {
            String errorQueueKey = errorQueueKey(message.getPubAgentName(), message.getSubSlingId(), message.getSubAgentName());
            OffsetQueue<Long> errorQueue = errorQueues.computeIfAbsent(errorQueueKey, key -> new OffsetQueueImpl<>());
            errorQueue.putItem(info.getOffset(), message.getOffset());
        }
    }

    @Nonnull
    private String errorQueueKey(String pubAgentName, String subSlingId, String subAgentName) {
        return String.format("%s#%s#%s", pubAgentName, subSlingId, subAgentName);
    }

    private void sendClearCommand(String subSlingId, String subAgentName, long offset) {
        ClearCommand commandMessage = ClearCommand.builder()
                .subSlingId(subSlingId)
                .subAgentName(subAgentName)
                .offset(offset)
                .build();
        LOG.info("Sending clear command to subSlingId: {}, subAgentName: {} with offset {}.", subSlingId, subAgentName, offset);
        sender.accept(commandMessage);
    }

}
