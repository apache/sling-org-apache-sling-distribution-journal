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

import static org.apache.sling.distribution.journal.HandlerAdapter.create;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.messages.ClearCommand;
import org.apache.sling.distribution.journal.shared.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandPoller implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CommandPoller.class);

    private final String subSlingId;
    private final String subAgentName;
    private final Closeable poller;
    private final AtomicLong clearOffset = new AtomicLong(-1);

    public CommandPoller(MessagingProvider messagingProvider, Topics topics, String subSlingId, String subAgentName) {
        this.subSlingId = subSlingId;
        this.subAgentName = subAgentName;
        this.poller = messagingProvider.createPoller(
                    topics.getCommandTopic(),
                    Reset.earliest,
                    create(ClearCommand.class, this::handleCommandMessage)
                    );
    }
    
    public boolean isCleared(long offset) {
        return offset <= clearOffset.longValue();
    }

    private void handleCommandMessage(MessageInfo info, ClearCommand message) {
        if (!subSlingId.equals(message.getSubSlingId()) || !subAgentName.equals(message.getSubAgentName())) {
            LOG.debug("Skip command for subSlingId {}", message.getSubSlingId());
            return;
        }

        handleClearCommand(message.getOffset());
    }

    private void handleClearCommand(long offset) {
        long oldOffset = clearOffset.get();
        long newOffset = updateClearOffsetIfLarger(offset);
        LOG.info("Handled clear command for offset {}. Old clear offset was {}, new clear offset is {}.", offset, oldOffset, newOffset);
    }

    private long updateClearOffsetIfLarger(long offset) {
        return clearOffset.accumulateAndGet(offset, Math::max);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(poller);
    }
}
