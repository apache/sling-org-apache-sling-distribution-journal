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
package org.apache.sling.distribution.journal.impl.subscriber;

import java.io.Closeable;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.messages.Messages.SubscriberConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.sling.distribution.journal.impl.queue.impl.PackageRetries;
import org.apache.sling.distribution.journal.messages.Messages;
import org.apache.sling.distribution.journal.messages.Messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.Messages.SubscriberState;
import org.apache.sling.distribution.journal.MessageSender;

@ParametersAreNonnullByDefault
class Announcer implements Runnable, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Announcer.class);

    private final String topicName;

    private final LocalStore offsetStore;

    private final MessageSender<DiscoveryMessage> sender;

    private final String subSlingId;

    private final String subAgentName;

    private final Set<String> pubAgentNames;

    private final PackageRetries packageRetries;

    private final boolean editable;

    private final int maxRetries;

    private final ScheduledExecutorService executor;

    public Announcer(String subSlingId,
                     String subAgentName,
                     String topicName,
                     Set<String> pubAgentNames,
                     MessageSender<DiscoveryMessage> disSender,
                     LocalStore offsetStore,
                     PackageRetries packageRetries,
                     int maxRetries,
                     boolean editable,
                     int announceDelay) {
        this.subSlingId = Objects.requireNonNull(subSlingId);
        this.subAgentName = Objects.requireNonNull(subAgentName);
        this.topicName = Objects.requireNonNull(topicName);
        this.pubAgentNames = Objects.requireNonNull(pubAgentNames);
        this.sender = Objects.requireNonNull(disSender);
        this.offsetStore = Objects.requireNonNull(offsetStore);
        this.packageRetries = Objects.requireNonNull(packageRetries);
        this.maxRetries = maxRetries;
        this.editable = editable;
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(this, 0, announceDelay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        LOG.debug("Sending discovery message for agent {}", subAgentName);
        try {

            long offset = offsetStore.load("offset", -1L);

            SubscriberConfiguration subscriberConfiguration = SubscriberConfiguration.newBuilder()
                    .setEditable(editable)
                    .setMaxRetries(maxRetries)
                    .build();
            DiscoveryMessage.Builder disMsgBuilder = DiscoveryMessage
                    .newBuilder()
                    .setSubSlingId(subSlingId)
                    .setSubAgentName(subAgentName)
                    .setSubscriberConfiguration(subscriberConfiguration);
            for (String pubAgentName : pubAgentNames) {
                int retries = packageRetries.get(pubAgentName);
                disMsgBuilder.addSubscriberState(createOffset(pubAgentName, offset, retries));
            }

            sender.send(topicName, disMsgBuilder.build());
        } catch (Throwable e) {
            String msg = String.format("Failed to send discovery message for agent %s, %s", subAgentName, e.getMessage());
            LOG.info(msg, e);
        }
    }

    private SubscriberState createOffset(String pubAgentName, long offset, int retries) {
        return Messages.SubscriberState.newBuilder()
                .setPubAgentName(pubAgentName)
                .setRetries(retries)
                .setOffset(offset)
                .build();
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
