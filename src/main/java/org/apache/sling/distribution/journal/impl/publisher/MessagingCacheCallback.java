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
package org.apache.sling.distribution.journal.impl.publisher;

import static org.apache.sling.distribution.journal.HandlerAdapter.create;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.discovery.DiscoveryService;
import org.apache.sling.distribution.journal.impl.discovery.State;
import org.apache.sling.distribution.journal.impl.discovery.TopologyView;
import org.apache.sling.distribution.journal.messages.ClearCommand;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.queue.CacheCallback;
import org.apache.sling.distribution.journal.queue.ClearCallback;
import org.apache.sling.distribution.journal.queue.QueueState;
import org.apache.sling.distribution.journal.shared.AgentId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagingCacheCallback implements CacheCallback {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    private final MessagingProvider messagingProvider;

    private final String packageTopic;

    private final PublishMetrics publishMetrics;

    private final DiscoveryService discoveryService;

    private final Consumer<ClearCommand> commandSender;

    public MessagingCacheCallback(
            MessagingProvider messagingProvider, 
            String packageTopic, 
            PublishMetrics publishMetrics,
            DiscoveryService discoveryService,
            Consumer<ClearCommand> commandSender) {
        this.messagingProvider = messagingProvider;
        this.packageTopic = packageTopic;
        this.publishMetrics = publishMetrics;
        this.discoveryService = discoveryService;
        this.commandSender = commandSender;
    }

    @Override
    public Closeable createConsumer(MessageHandler<PackageMessage> handler) {
        log.info("Starting consumer");
        QueueCacheSeeder seeder = new QueueCacheSeeder(messagingProvider.createSender(packageTopic)); //NOSONAR
        Closeable poller = messagingProvider.createPoller( //NOSONAR
                packageTopic,
                Reset.latest,
                create(PackageMessage.class, (info, message) -> { seeder.close(); handler.handle(info, message); }) 
                ); 
        seeder.startSeeding();
        return () -> IOUtils.closeQuietly(seeder, poller);
    }
    
    @Override
    public List<FullMessage<PackageMessage>> fetchRange(long minOffset, long maxOffset) throws InterruptedException {
        publishMetrics.getQueueCacheFetchCount().increment();
        return new RangePoller(messagingProvider, packageTopic, minOffset, maxOffset, RangePoller.DEFAULT_SEED_DELAY_SECONDS)
                .fetchRange();
    }

    @Override
    public QueueState getQueueState(String pubAgentName, String subAgentId) {
        TopologyView view = discoveryService.getTopologyView();
        State state = view.getState(subAgentId, pubAgentName);
        if (state == null) {
            return null;
        }
        ClearCallback editableCallback = offset -> sendClearCommand(pubAgentName, new AgentId(subAgentId), offset);
        ClearCallback clearCallback = state.isEditable() ? editableCallback : null;
        long curOffset = state.getOffset();
        int headRetries = state.getRetries();
        int maxRetries = state.getMaxRetries();
        return new QueueState(curOffset, headRetries, maxRetries, clearCallback);
    }
    
    private void sendClearCommand(String pubAgentName, AgentId subAgentId, long offset) {
        ClearCommand command = ClearCommand.builder()
                .pubAgentName(pubAgentName)
                .subSlingId(subAgentId.getSlingId())
                .subAgentName(subAgentId.getAgentName())
                .offset(offset)
                .build();
        log.info("Sending clear command {}", command);
        commandSender.accept(command);
    }

    @Override
    public Set<String> getSubscribedAgentIds(String pubAgentName) {
        return discoveryService.getTopologyView().getSubscribedAgentIds(pubAgentName);
    }
}
