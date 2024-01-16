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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * A DistributionSubscriber is considered ready when it is idle for more than
 * the READY_IDLE_TIME_SECONDS at least once ; or when it is busy processing
 * the same package for more than MAX_RETRIES times.
 */
public class SubscriberIdle implements IdleCheck {
    public static final int DEFAULT_IDLE_TIME_MILLIS = 10 * 1000;
    public static final int DEFAULT_FORCE_IDLE_MILLIS = 5 * 60 * 1000;

    private static final int ACCEPTABLE_AGE_DIFF_MS = 120 * 1000;
    public static final int MAX_RETRIES = 10;

    private final int idleMillis;
    private final AtomicBoolean isReady;
    private final Supplier<Long> timeProvider;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> schedule;

    public SubscriberIdle(int idleMillis, int forceIdleMillies, AtomicBoolean readyHolder, Supplier<Long> timeProvider) {
        this.idleMillis = idleMillis;
        this.isReady = readyHolder;
        this.timeProvider = timeProvider;
        executor = Executors.newScheduledThreadPool(2);
        executor.schedule(this::forceIdle, forceIdleMillies, TimeUnit.MILLISECONDS);
        idle();
    }
    
    @Override
    public boolean isIdle() {
        return isReady.get();
    }
    
    /**
     * {@inheritDoc}
     */
    public synchronized void busy(int retries, long messageCreateTime) {
        if (timeProvider.get() - messageCreateTime < ACCEPTABLE_AGE_DIFF_MS) {
            ready();
        }
        cancelSchedule();
        if (retries > MAX_RETRIES) {
            ready();
        }
    }

    /**
     * {@inheritDoc}
     */
    public synchronized void idle() {
        if (!isReady.get()) {
            cancelSchedule();
            if (!executor.isShutdown()) {
                schedule = executor.schedule(this::ready, idleMillis, TimeUnit.MILLISECONDS);
            }
        }
    }
    
    private void forceIdle() {
        isReady.set(true);
        cancelSchedule();
    }
    
    private void cancelSchedule() {
        if (schedule != null) {
            schedule.cancel(false);
        }
    }

    private void ready() {
        isReady.set(true);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

}
