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
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    
    private Random random;
    private int maxDelay;

    private int currentMaxDelay;
    
    public ExponentialBackOff(Duration startDelay, Duration maxDelay, Runnable checkCallback) {
        this.currentMaxDelay = asMS(startDelay);
        this.maxDelay = asMS(maxDelay);
        this.checkCallback = checkCallback;
        this.executor = Executors.newScheduledThreadPool(1);
        this.random = new Random();
        scheduleCheck();
    }

    private int asMS(Duration startDelay) {
        return new Long(startDelay.toMillis()).intValue();
    }

    @Override
    public synchronized void close() {
        this.executor.shutdown();
    }

    private void scheduleCheck() {
        this.currentMaxDelay = Math.min(this.currentMaxDelay *2, maxDelay);
        int delay = this.random.nextInt(currentMaxDelay) + 1;
        log.info("Scheduling next check in {} ms with maximum {} ms.", delay, currentMaxDelay);
        this.executor.schedule(this::check, delay, TimeUnit.MILLISECONDS);
    }

    private void check() {
        try {
            this.checkCallback.run();
            this.close();
        } catch (RuntimeException e) {
            scheduleCheck();
        }
    }
}
