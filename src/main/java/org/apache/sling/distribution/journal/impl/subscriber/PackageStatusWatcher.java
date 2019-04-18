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


import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.Messages;
import org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.sling.distribution.journal.HandlerAdapter.create;

public class PackageStatusWatcher implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PackageStatusWatcher.class);

    private final Closeable poller;
    private final String topicName;
    private final String subAgentName;
    private final NavigableMap<Long, FullMessage<PackageStatusMessage>> cache = new ConcurrentSkipListMap<>();

    public PackageStatusWatcher(MessagingProvider messagingProvider, Topics topics, String subAgentName) {
        this.topicName = topics.getStatusTopic();
        this.subAgentName = subAgentName;

        poller = messagingProvider.createPoller(
                topicName,
                Reset.earliest,
                create(Messages.PackageStatusMessage.class, this::handle)
        );
    }


    /**
     * Gets the status that confirms the package at offset pkgOffset
     * @param pkgOffset the offset of the package
     * @return the status confirming the package; or null if it has not been confirmed yet
     */
    public PackageStatusMessage.Status getStatus(long pkgOffset) {
        FullMessage<PackageStatusMessage> msg = cache.get(pkgOffset);

        return msg != null ? msg.getMessage().getStatus() : null;
    }

    /**
     * Gets the status offset that confirms the packages at offset pkgOffset
     * @param pkgOffset the offset of the package
     * @return the offset of the confirming package; or null if has not been confirmed yet
     */
    public Long getStatusOffset(long pkgOffset) {
        FullMessage<PackageStatusMessage> msg = cache.get(pkgOffset);

        return msg != null ? msg.getInfo().getOffset() : null;
    }

    /**
     * Clear all offsets in the cache smaller to the given pkgOffset.
     * @param pkgOffset
     */
    public void clear(long pkgOffset) {
        NavigableMap<Long, FullMessage<PackageStatusMessage>> removed = cache.headMap(pkgOffset, false);
        if (! removed.isEmpty()) {
            LOG.info("Remove package offsets {} from status cache", removed.keySet());
        }
        removed.clear();
    }

    @Override
    public void close() throws IOException {
        poller.close();
    }

    public void handle(MessageInfo info, Messages.PackageStatusMessage msg) {
        // TODO: check revision

        Long pkgOffset = msg.getOffset();
        FullMessage<PackageStatusMessage> message = new FullMessage<>(info, msg);

        // cache only messages that are from the given subAgentName
        if (!subAgentName.equals(msg.getSubAgentName())) {
            return;
        }

        if (cache.containsKey(pkgOffset)) {
            LOG.warn("Package offset {} already exists", pkgOffset);
        }

        cache.put(pkgOffset, message);
    }
}