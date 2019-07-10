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
package org.apache.sling.distribution.journal.impl.shared;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retry with exponential backoff.
 * 
 * Calls checkCallback until it does not throw an Exception.
 * Retries are first done with startDelay, then doubled until maxDelay is reached.
 */
public class ExponentialBackOff implements Closeable {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ScheduledExecutorService executor;
    private final Runnable checkCallback;
    
    private ThreadLocalRandom random;
    private long startDelay;
    private long maxDelay;

    private long currentMaxDelay;
    private AtomicBoolean isScheduled;
    private long lastCheck;

    private boolean randomDelay;

    
    public ExponentialBackOff(Duration startDelay, Duration maxDelay, boolean randomDelay, Runnable checkCallback) {
        this.randomDelay = randomDelay;
        this.startDelay = startDelay.toMillis();
        this.currentMaxDelay = this.startDelay;
        this.maxDelay = maxDelay.toMillis();
        this.checkCallback = checkCallback;
        this.executor = Executors.newScheduledThreadPool(1);
        this.random = ThreadLocalRandom.current();
        this.isScheduled = new AtomicBoolean();
        this.lastCheck = 0;
    }

    @Override
    public void close() {
        this.executor.shutdown();
    }
    
    public void startChecks() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - this.lastCheck > this.currentMaxDelay * 2) {
            // Only reset delay if we were not called recently
            log.info("Starting with initial delay {}", this.startDelay);
            this.currentMaxDelay = this.startDelay;
        }
        scheduleCheck();
    }

    private synchronized void scheduleCheck() {
        if (isScheduled.compareAndSet(false, true)) {
            long delay = this.randomDelay ? this.random.nextLong(currentMaxDelay) + 1 : currentMaxDelay;
            log.info("Scheduling next check in {} ms with maximum {} ms.", delay, currentMaxDelay);
            this.executor.schedule(this::check, delay, TimeUnit.MILLISECONDS);
            this.currentMaxDelay = Math.min(this.currentMaxDelay * 2, maxDelay);
        }
    }

    private void check() {
        try {
            this.lastCheck = System.currentTimeMillis();
            this.isScheduled.set(false);
            this.checkCallback.run();
        } catch (RuntimeException e) {
            scheduleCheck();
        }
    }
}
