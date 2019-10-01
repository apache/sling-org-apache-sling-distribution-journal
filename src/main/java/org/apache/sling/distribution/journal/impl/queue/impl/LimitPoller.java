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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.messages.Messages;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ParametersAreNonnullByDefault
public class LimitPoller {
    private final Logger log = LoggerFactory.getLogger(LimitPoller.class);
    
    private final long maxMessages;
    private final Closeable headPoller;
    private final List<FullMessage<PackageMessage>> messages;
    private final Semaphore nextMessage;
    
    public LimitPoller(MessagingProvider messagingProvider,
                          String packageTopic,
                          long minOffset,
                          long maxMessages) {
        this.maxMessages = maxMessages;
        this.messages = new ArrayList<>();
        this.nextMessage = new Semaphore(0);
        String assign = messagingProvider.assignTo(minOffset);
        log.info("Fetching {} messages starting from {}", maxMessages, minOffset);
        headPoller = messagingProvider.createPoller(
                packageTopic, Reset.earliest, assign,
                create(Messages.PackageMessage.class, this::handlePackage));
    }

    public List<FullMessage<PackageMessage>> fetch(int timeOutMs) {
        try {
            boolean timeout = false;
            while (!timeout && this.messages.size() < maxMessages) {
                timeout = !nextMessage.tryAcquire(timeOutMs, TimeUnit.MILLISECONDS);
            }
            ArrayList<FullMessage<PackageMessage>> result = new ArrayList<>(messages);
            log.info("Fetched {} messages", result.size());
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(headPoller);
        }
    }

    private void handlePackage(MessageInfo info, Messages.PackageMessage message) {
        long offset = info.getOffset();
        log.debug("Reading offset {}", offset);
        if (this.messages.size() < maxMessages) {
            messages.add(new FullMessage<>(info, message));
        }
        nextMessage.release();
    }

}
