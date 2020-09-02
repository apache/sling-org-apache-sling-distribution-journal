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

import java.io.Closeable;
import java.util.Hashtable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.felix.systemready.CheckStatus;
import org.apache.felix.systemready.CheckStatus.State;
import org.apache.felix.systemready.StateType;
import org.apache.felix.systemready.SystemReadyCheck;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

/**
 * A DistributionSubscriber is considered ready when it is idle for more than
 * the READY_IDLE_TIME_SECONDS at least once ; or when it is busy processing
 * the same package for more than MAX_RETRIES times.
 */
public class SubscriberIdle implements SystemReadyCheck, Closeable {
    public static final int DEFAULT_IDLE_TIME_MILLIS = 10000;

    public static final int MAX_RETRIES = 10;

    private final int idleMillis;
    private final AtomicBoolean isReady;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> schedule;

    private final ServiceRegistration<SystemReadyCheck> reg;
    
    public SubscriberIdle(BundleContext context, int idleMillis, AtomicBoolean readyHolder) {
        this.idleMillis = idleMillis;
        this.isReady = readyHolder;
        executor = Executors.newScheduledThreadPool(1);
        idle();
        this.reg = context.registerService(SystemReadyCheck.class, this, new Hashtable<>());
    }
    
    @Override
    public String getName() {
        return "DistributionSubscriber idle";
    }

    @Override
    public CheckStatus getStatus() {
        State state = isReady.get() ? State.GREEN : State.RED; 
        return new CheckStatus(getName(), StateType.READY, state, "DistributionSubscriber idle");
    }
    
    /**
     * Called when processing of a message starts
     *
     * @param retries the number of retries to process the message
     */
    public synchronized void busy(int retries) {
        cancelSchedule();
        if (retries > MAX_RETRIES) {
            ready();
        }
    }

    /**
     * Called when processing of a message has finished
     */
    public synchronized void idle() {
        if (!isReady.get()) {
            cancelSchedule();
            if (!executor.isShutdown()) {
                schedule = executor.schedule(this::ready, idleMillis, TimeUnit.MILLISECONDS);
            }
        }
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
        if (reg != null) {
            reg.unregister();
        }
    }

}
