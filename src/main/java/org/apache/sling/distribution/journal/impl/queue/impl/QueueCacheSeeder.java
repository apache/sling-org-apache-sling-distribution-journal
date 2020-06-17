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

import java.io.Closeable;
import java.util.UUID;
import java.util.function.LongConsumer;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.distribution.journal.HandlerAdapter.create;
import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;

public class QueueCacheSeeder implements Closeable {


    private static final Logger LOG = LoggerFactory.getLogger(QueueCacheSeeder.class);

    /**
     * Interval in millisecond between two seeding messages to seed the cache.
     */
    private static final long CACHE_SEEDING_DELAY_MS = 10_000;

    private final String topic;

    private final MessagingProvider provider;

    private volatile Closeable poller;

    private volatile boolean closed;

    public QueueCacheSeeder(MessagingProvider provider, String topic) {
        this.provider = provider;
        this.topic = topic;
    }

    public void seedOne() {
        startBackgroundThread(this::sendSeedingMessage, "Seeder thread - one seed");
    }

    public void seed(LongConsumer callback) {
        poller = provider.createPoller(topic, Reset.latest,
                create(PackageMessage.class, (info, msg) -> {
                    close();
                    callback.accept(info.getOffset());
                }));
        startBackgroundThread(this::sendSeedingMessages, "Seeder thread");
    }

    @Override
    public void close() {
        closed = true;
        IOUtils.closeQuietly(poller);
    }

    private void sendSeedingMessages() {
        LOG.info("Start message seeder");
        try {
            MessageSender<PackageMessage> sender = provider.createSender(topic);
            while (!closed) {
                sendSeedingMessage(sender);
                delay(CACHE_SEEDING_DELAY_MS);
            }
        } finally {
            LOG.info("Stop message seeder");
        }
    }

    private void sendSeedingMessage() {
        sendSeedingMessage(provider.createSender(topic));
    }

    private void sendSeedingMessage(MessageSender<PackageMessage> sender) {
        PackageMessage pkgMsg = createTestMessage();
        LOG.info("Send seeding message");
        try {
            sender.send(pkgMsg);
        } catch (MessagingException e) {
            LOG.warn(e.getMessage(), e);
            delay(CACHE_SEEDING_DELAY_MS * 10);
        }
    }

    private static void delay(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected PackageMessage createTestMessage() {
        String pkgId = UUID.randomUUID().toString();
        return PackageMessage.builder()
                .pubSlingId("seeder")
                .pkgId(pkgId)
                .pkgType("seeder")
                .reqType(PackageMessage.ReqType.TEST)
                .build();
    }
}
