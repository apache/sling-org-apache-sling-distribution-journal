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
import java.util.concurrent.atomic.AtomicLong;

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
public class RangePoller {

    private static final Logger LOG = LoggerFactory.getLogger(RangePoller.class);

    private final long maxOffset;

    private final long minOffset;

    private final Closeable headPoller;

    private final List<FullMessage<PackageMessage>> messages;

    private final Semaphore nextMessage;
    private final AtomicLong lastMessageTime;
    private final AtomicLong lastOffset;
    private final AtomicLong numMessages;
    
    public RangePoller(MessagingProvider messagingProvider,
                          String packageTopic,
                          long minOffset,
                          long maxOffsetExclusive) {
        this.maxOffset = maxOffsetExclusive;
        this.minOffset = minOffset;
        this.messages = new ArrayList<>();
        this.nextMessage = new Semaphore(0);
        this.lastMessageTime = new AtomicLong(System.currentTimeMillis());
        this.lastOffset = new AtomicLong();
        this.numMessages = new AtomicLong();
        String assign = messagingProvider.assignTo(minOffset);
        LOG.info("Fetching offsets [{},{}[", minOffset, maxOffsetExclusive);
        headPoller = messagingProvider.createPoller(
                packageTopic, Reset.earliest, assign,
                create(Messages.PackageMessage.class, this::handlePackage));
    }

    public List<FullMessage<PackageMessage>> fetchRange() throws InterruptedException {
        return fetchRange(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }
    
    public List<FullMessage<PackageMessage>> fetchRange(int maxMessages, int timeOutMs) throws InterruptedException {
        try {
            boolean timeout = false;
            while (lastOffset.get() < maxOffset && !timeout && this.numMessages.get() < maxMessages) {
                timeout = !nextMessage.tryAcquire(timeOutMs, TimeUnit.MILLISECONDS);
            }
            if (timeout) {
                LOG.info("Timeout fetching messages. Got messages from {} to {}. Number of messages: {}", minOffset, lastOffset,messages.size());
            } else {
                LOG.info("Fetched offsets [{},{}[. Number of messages: {}", minOffset, maxOffset, messages.size());
            }
            return new ArrayList<>(messages);
        } finally {
            IOUtils.closeQuietly(headPoller);
        }
    }

    private void handlePackage(MessageInfo info, Messages.PackageMessage message) {
        long offset = info.getOffset();
        LOG.debug("Reading offset {}", offset);
        this.lastMessageTime.set(System.currentTimeMillis());
        this.lastOffset.set(offset);
        if (offset < maxOffset) {
            this.numMessages.incrementAndGet();
            messages.add(new FullMessage<>(info, message));
        }
        nextMessage.release();
    }

}
