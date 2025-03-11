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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DistributionSubscriber is considered ready when one of the conditions is met:
 * <ul>
 * <li>is idle (no further message received) for more than the idleMillis at least once
 * <li>is busy processing the same package for more than MAX_RETRIES times. (blocked queue assumed)
 * <li>received message created time is less than 120 seconds
 * <li>DEFAULT_FORCE_IDLE_MILLIS time has passed
 * </ul>
 * After becoming ready once, the check stays ready.
 */
public class SubscriberReady implements IdleCheck {
    public static final int MAX_RETRIES = 10;
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String subAgentName;
    private final long idleMillis;
    private final long acceptableAgeDiffMs;
    private final AtomicBoolean isReady;
    private final Supplier<Long> timeProvider;
    private final ScheduledExecutorService executor;
    private final long forceIdleMillies;
    private final long startTime;

    private ScheduledFuture<?> schedule;
    private final ScheduledFuture<?> forceShedule;

    public SubscriberReady(String subAgentName, long idleMillis, long forceIdleMillies, long acceptableAgeDiffMs, AtomicBoolean readyHolder, Supplier<Long> timeProvider) {
        this.subAgentName = subAgentName;
        this.idleMillis = idleMillis;
        this.forceIdleMillies = forceIdleMillies;
        this.acceptableAgeDiffMs = acceptableAgeDiffMs;
        this.isReady = readyHolder;
        this.timeProvider = timeProvider;
        this.startTime = timeProvider.get();
        executor = Executors.newScheduledThreadPool(2);
        forceShedule = executor.schedule(this::forceIdle, forceIdleMillies, TimeUnit.MILLISECONDS);
        idle();
        log.info("Started");
    }
    
    @Override
    public boolean isReady() {
        return isReady.get();
    }
    
    /**
     * {@inheritDoc}
     */
    public synchronized void busy(int retries, long messageCreateTime) {
        if (isReady()) {
            return;
        }
        cancelSchedule();
        long latency = timeProvider.get() - messageCreateTime;
        if (latency < acceptableAgeDiffMs) {
            ready(String.format("Package message latency %d s < %d s acceptable limit", MILLISECONDS.toSeconds(latency), MILLISECONDS.toSeconds(acceptableAgeDiffMs)));
        }
        if (retries > MAX_RETRIES) {
            ready(String.format("Retries %d > %d", retries, MAX_RETRIES));
        }
    }

    /**
     * {@inheritDoc}
     */
    public synchronized void idle() {
        if (isReady()) {
            return;
        }
        cancelSchedule();
        if (!executor.isShutdown()) {
            schedule = executor.schedule(this::idleReady, idleMillis, MILLISECONDS);
        }
    }
    
    private void forceIdle() {
        ready(String.format("Forcing ready after %d s", MILLISECONDS.toSeconds(forceIdleMillies)));
        cancelSchedule();
    }
    
    private void cancelSchedule() {
        if (schedule != null) {
            schedule.cancel(false);
        }
    }

    private void idleReady() {
        ready(String.format("%s ready after being idle for > %d s", subAgentName, MILLISECONDS.toSeconds(idleMillis)));
    }
    
    private void ready(String reason) {
        long readyTime = timeProvider.get();
        long timeToIdle = MILLISECONDS.toSeconds(readyTime - startTime);
        log.info("Subscriber becoming ready after timeToIdle={} s. Reason='{}'", timeToIdle, reason);
        isReady.set(true);
        cancelSchedule();
        forceShedule.cancel(false);
    }

    @Override
    public void close() {
        forceShedule.cancel(false);
        cancelSchedule();
        executor.shutdownNow();
    }

}
