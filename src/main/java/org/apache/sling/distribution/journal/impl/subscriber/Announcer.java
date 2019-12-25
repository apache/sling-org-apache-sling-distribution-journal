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
import java.util.function.Consumer;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.messages.Messages;
import org.apache.sling.distribution.journal.messages.Messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.Messages.SubscriberConfiguration;
import org.apache.sling.distribution.journal.messages.Messages.SubscriberState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ParametersAreNonnullByDefault
class Announcer implements Runnable, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Announcer.class);

    private final BookKeeper bookKeeper;

    private final Consumer<DiscoveryMessage> sender;

    private final String subSlingId;

    private final String subAgentName;

    private final Set<String> pubAgentNames;

    private final boolean editable;

    private final int maxRetries;

    private final ScheduledExecutorService executor;

    public Announcer(String subSlingId,
                     String subAgentName,
                     Set<String> pubAgentNames,
                     Consumer<DiscoveryMessage> disSender,
                     BookKeeper bookKeeper,
                     int maxRetries,
                     boolean editable,
                     int announceDelay) {
        this.subSlingId = Objects.requireNonNull(subSlingId);
        this.subAgentName = Objects.requireNonNull(subAgentName);
        this.pubAgentNames = Objects.requireNonNull(pubAgentNames);
        this.sender = Objects.requireNonNull(disSender);
        this.bookKeeper = Objects.requireNonNull(bookKeeper);
        this.maxRetries = maxRetries;
        this.editable = editable;
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(this, 0, announceDelay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        LOG.debug("Sending discovery message for agent {}", subAgentName);
        try {
            DiscoveryMessage msg = createDiscoveryMessage();
            sender.accept(msg);
        } catch (Exception e) {
            LOG.info("Failed to send discovery message for agent {}, {}", subAgentName, e.getMessage(), e);
        }
    }

    private DiscoveryMessage createDiscoveryMessage() {
        long offset = bookKeeper.loadOffset();
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
            disMsgBuilder.addSubscriberState(subscriberState(pubAgentName, offset));
        }
        return disMsgBuilder.build();
    }

    private SubscriberState subscriberState(String pubAgentName, long offset) {
        int retries = bookKeeper.getRetries(pubAgentName);
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
