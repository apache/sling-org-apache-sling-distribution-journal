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

import static org.apache.sling.distribution.journal.impl.subscriber.SubscriberReady.MAX_RETRIES;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SubscriberReadyTest {

    private static final int IDLE_MILLIES = 40;
    private static final int FORCE_IDLE_MILLIS = 500;
    private static final int TIME_NOW = 1000000;
    private static final int TEN_MINS_AGO = TIME_NOW - 10 * 60 * 1000;
    private static final int NO_RETRIES = 0;
    
    private SubscriberReady idle;
    private AtomicLong timeProvider = new AtomicLong(TIME_NOW);

    @Before
    public void before() {
        idle = new SubscriberReady(IDLE_MILLIES, FORCE_IDLE_MILLIS, new AtomicBoolean(), timeProvider::get);
    }

    @After
    public void after() {
        idle.close();
    }
    
    @Test
    public void testIdle() {
        assertThat("Initial state", idle.isReady(), equalTo(false));
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        idle.idle();
        assertThat("State after reset", idle.isReady(), equalTo(false));
        
        sleep(30);
        assertThat("State after time below idle limit", idle.isReady(), equalTo(false));
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        
        sleep(80);
        idle.idle();
        assertThat("State after long processing", idle.isReady(), equalTo(false));
        
        sleep(80);
        assertThat("State after time over idle limit", idle.isReady(), equalTo(true));
        
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        assertThat("State should not be reset once it reached GREEN", idle.isReady(), equalTo(true));
    }

    @Test
    public void testIdleAndNotForceIdle() throws InterruptedException {
        assertThat("Initial state", idle.isReady(), equalTo(false));
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        assertThat("State after reset", idle.isReady(), equalTo(false));
        idle.idle();
        sleep(FORCE_IDLE_MILLIS * 2);
        assertThat("State after time over idle limit", idle.isReady(), equalTo(true));
        // Observe log to make sure force idle is not logged after becoming ready because of normal idle time
        
    }
    
    @Test
    public void testForceIdle() throws InterruptedException {
        assertThat("Initial state", idle.isReady(), equalTo(false));
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        assertThat("State after reset", idle.isReady(), equalTo(false));
        sleep(FORCE_IDLE_MILLIS * 2);
        assertThat("State after time over idle limit", idle.isReady(), equalTo(true));
        // Observe log to make sure force idle is not logged after becoming ready because of normal idle time
        
    }

    
    @Test
    public void testShortIdleTimeButCreateTimeNearCurrentTime() throws InterruptedException {
        assertThat("Initial state", idle.isReady(), equalTo(false));
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        idle.idle();
        assertThat("State after reset", idle.isReady(), equalTo(false));
        
        idle.busy(NO_RETRIES, TIME_NOW - 1000);
        assertThat("Create time near TIME_NOW should return idle", idle.isReady(), equalTo(true));
    }

    /** 
     * In case of blocked queue (retries > MAX_RETRIES) we should report ready
     */
    @Test
    public void testMaxRetries() {
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        idle.idle();
        assertThat("State with no retries", idle.isReady(), equalTo(false));
        idle.busy(MAX_RETRIES, TEN_MINS_AGO);
        idle.idle();
        assertThat("State with retries <= MAX_RETRIES", idle.isReady(), equalTo(false));
        idle.busy(MAX_RETRIES + 1, TEN_MINS_AGO);
        idle.idle();
        assertThat("State with retries > MAX_RETRIES", idle.isReady(), equalTo(true));
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        assertThat("State should not be reset once it reached idle", idle.isReady(), equalTo(true));
    }
    
    @Test
    public void testStartIdle() throws InterruptedException {
        assertThat("Initial state", idle.isReady(), equalTo(false));
        sleep(IDLE_MILLIES * 2);
        assertThat("State after time over idle limit", idle.isReady(), equalTo(true));
        idle.close();
    }

    private void sleep(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Should not happen");
        }
    }
    
}
