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
package org.apache.sling.distribution.journal.shared;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.Closeable;
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

    private final long startDelay;
    private final long maxDelay;
    private final boolean randomDelay;
    private final Runnable checkCallback;

    private final ScheduledExecutorService executor;
    private final AtomicBoolean isScheduled;
    
    private long currentMaxDelay;
    private long lastCheck;

    /**
     * @param startDelay the start delay in milliseconds
     * @param maxDelay the max delay in milliseconds
     * @param randomDelay {@code true} to randomise the delay between 0 and the current max delay ;
     *                    {@code false} to use the current max delay.
     * @param checkCallback the code to invoke when the current delay has elapsed
     */
    public ExponentialBackOff(long startDelay, long maxDelay, boolean randomDelay, Runnable checkCallback) {
        this.startDelay = startDelay;
        this.maxDelay = maxDelay;
        this.randomDelay = randomDelay;
        this.checkCallback = checkCallback;
        this.executor = Executors.newScheduledThreadPool(1);
        this.currentMaxDelay = this.startDelay;
        this.isScheduled = new AtomicBoolean();
        this.lastCheck = 0;
    }

    @Override
    public void close() {
        log.info("Shutting down exponential backoff executor");
        this.executor.shutdown();
        this.executor.shutdownNow();
        try {
            this.executor.awaitTermination(100, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Shutdown completed");
    }
    
    public void startChecks() {
        if (noRecentErrors()) {
            log.info("No recent errors. Starting with initial delay {}", this.startDelay);
            this.currentMaxDelay = this.startDelay;
        }
        scheduleCheck();
    }

    private boolean noRecentErrors() {
        long timeSinceLastError = System.currentTimeMillis() - this.lastCheck;
        return timeSinceLastError  > this.currentMaxDelay * 2;
    }

    private synchronized void scheduleCheck() {
        if (isScheduled.compareAndSet(false, true)) {
            long delay = computeDelay();
            log.info("Scheduling next check in {} ms with maximum delay of {} ms.", delay, currentMaxDelay);
            this.executor.schedule(this::check, delay, MILLISECONDS);
            this.currentMaxDelay = Math.min(this.currentMaxDelay * 2, maxDelay);
        }
    }

    private long computeDelay() {
        return this.randomDelay ? ThreadLocalRandom.current().nextLong(currentMaxDelay) + 1 : currentMaxDelay;
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
