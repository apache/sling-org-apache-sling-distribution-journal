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

import static java.lang.String.format;
import static org.apache.sling.distribution.journal.HandlerAdapter.create;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.Messages.CommandMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandPoller implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DistributionSubscriber.class);

    private final String subSlingId;
    private final String subAgentName;
    private final Closeable commandPoller;
    private final AtomicLong clearOffset = new AtomicLong(-1);

    public CommandPoller(MessagingProvider messagingProvider, Topics topics, String subSlingId, String subAgentName, boolean editable) {
        this.subSlingId = subSlingId;
        this.subAgentName = subAgentName;
        if (editable) {

            /*
             * We currently only support commands requiring editable mode.
             * As an optimisation, we don't register a poller for non
             * editable subscribers.
             *
             * When supporting commands independent from editable mode,
             * this optimisation will be removed.
             */

            commandPoller = messagingProvider.createPoller(
                    topics.getCommandTopic(),
                    Reset.earliest,
                    create(CommandMessage.class, this::handleCommandMessage));
        } else {
            commandPoller = null;
        }
    }
    
    public boolean isCleared(long offset) {
        return offset <= clearOffset.longValue();
    }

    private void handleCommandMessage(MessageInfo info, CommandMessage message) {
        if (!subSlingId.equals(message.getSubSlingId()) || !subAgentName.equals(message.getSubAgentName())) {
            LOG.debug(format("Skip command for subSlingId %s", message.getSubSlingId()));
            return;
        }

        if (message.hasClearCommand()) {
            handleClearCommand(message.getClearCommand().getOffset());
        } else {
            LOG.warn("Unsupported command {}", message);
        }
    }

    private void handleClearCommand(long offset) {
        updateClearOffsetIfLarger(offset);
        LOG.info("Handled clear command for offset {}", offset);
    }

    private long updateClearOffsetIfLarger(long offset) {
        return clearOffset.accumulateAndGet(offset, Math::max);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(commandPoller);
    }
}
