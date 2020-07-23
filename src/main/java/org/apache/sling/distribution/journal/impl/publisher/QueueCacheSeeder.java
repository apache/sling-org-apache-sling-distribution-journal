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

import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;

import java.io.Closeable;
import java.util.UUID;

import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueCacheSeeder implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(QueueCacheSeeder.class);

    /**
     * Interval in millisecond between two seeding messages to seed the cache.
     */
    private static final long CACHE_SEEDING_DELAY_MS = 10_000;

    private static final int MAX_CACHE_SEEDING_DELAY_MS = 900_000; // 15 minutes

    private volatile boolean closed;

    private MessageSender<PackageMessage> sender;

    private Thread seedingThread;
    
    public QueueCacheSeeder(MessageSender<PackageMessage> sender) {
        this.sender = sender;
    }

    public void startSeeding() {
        seedingThread = startBackgroundThread(this::sendSeedingMessages, "Seeder thread");
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                seedingThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * We repeatedly send seeding messages as the first message is sometimes not received by the consumer.
     */
    private void sendSeedingMessages() {
        LOG.info("Start message seeder");
        int count = 1;
        long cacheSeedingDelay = CACHE_SEEDING_DELAY_MS;
        try {
            while (!closed) {
                LOG.info("Send seeding message {} then wait {} ms", count, cacheSeedingDelay);
                sendSeedingMessage();
                delay(cacheSeedingDelay);
                cacheSeedingDelay = Math.min(cacheSeedingDelay * 2, MAX_CACHE_SEEDING_DELAY_MS);
                count++;
            }
        } finally {
            LOG.info("Stop message seeder");
        }
    }

    private void sendSeedingMessage() {
        try {
            PackageMessage pkgMsg = createTestMessage();
            sender.send(pkgMsg);
        } catch (MessagingException e) {
            LOG.warn(e.getMessage(), e);
        }
    }

    /**
     * Sleep with handling of interrupts and quick exit in case of closed.
     * We do not interrupt the seeder thread from outside as this sometimes fails in the messaging impl code.
     * 
     * @param sleepMs milliseconds to sleep
     */
    private void delay(long sleepMs) {
        long sleepCycles = sleepMs / 100;
        for (int curCycle=0; curCycle < sleepCycles; curCycle++) {
            if (closed) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
