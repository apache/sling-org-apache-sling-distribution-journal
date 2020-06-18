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
import java.util.concurrent.CountDownLatch;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.messages.PackageMessage;

@ParametersAreNonnullByDefault
public class RangePoller {

    private static final Logger LOG = LoggerFactory.getLogger(RangePoller.class);

    private final long maxOffset;

    private final long minOffset;

    private final Closeable headPoller;

    private final CountDownLatch fetched = new CountDownLatch(1);

    private final List<FullMessage<PackageMessage>> messages;
    
    public RangePoller(MessagingProvider messagingProvider,
                          String packageTopic,
                          long minOffset,
                          long maxOffset) {
        this.maxOffset = maxOffset;
        this.minOffset = minOffset;
        this.messages = new ArrayList<>();
        String assign = messagingProvider.assignTo(minOffset);
        LOG.info("Fetching offsets [{},{}[", minOffset, maxOffset);
        headPoller = messagingProvider.createPoller(
                packageTopic, Reset.earliest, assign,
                create(PackageMessage.class, this::handlePackage)
                );
    }

    public List<FullMessage<PackageMessage>> fetchRange() throws InterruptedException {
        try {
            fetched.await();
            LOG.info("Fetched offsets [{},{}[", minOffset, maxOffset);
            return messages;
        } finally {
            IOUtils.closeQuietly(headPoller);
        }
    }

    private void handlePackage(MessageInfo info, PackageMessage message) {
        long offset = info.getOffset();
        LOG.debug("Reading offset {}", offset);
        if (offset < maxOffset) {
            if (isNotTestMessage(message)) {
                messages.add(new FullMessage<>(info, message));
            }
        } else {
            fetched.countDown();
        }
    }
    
    private boolean isNotTestMessage(PackageMessage message) {
        return message.getReqType() != PackageMessage.ReqType.TEST;
    }
}
