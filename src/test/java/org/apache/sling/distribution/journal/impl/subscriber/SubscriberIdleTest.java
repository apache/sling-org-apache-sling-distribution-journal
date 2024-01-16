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

import static org.apache.sling.distribution.journal.impl.subscriber.SubscriberIdle.MAX_RETRIES;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SubscriberIdleTest {

    private static final int IDLE_MILLIES = 40;
    private static final int TIME_NOW = 1000000;
    private static final int TEN_MINS_AGO = TIME_NOW - 10 * 60 * 1000;
    private static final int NO_RETRIES = 0;
    
    private SubscriberIdle idle;
    private AtomicLong timeProvider = new AtomicLong(TIME_NOW);

    @Before
    public void before() {
        idle = new SubscriberIdle(IDLE_MILLIES, SubscriberIdle.DEFAULT_FORCE_IDLE_MILLIS, new AtomicBoolean(), timeProvider::get);
    }

    @After
    public void after() {
        idle.close();
    }
    
    @Test
    public void testIdle() throws InterruptedException {
        assertThat("Initial state", idle.isIdle(), equalTo(false));
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        idle.idle();
        assertThat("State after reset", idle.isIdle(), equalTo(false));
        
        Thread.sleep(30);
        assertThat("State after time below idle limit", idle.isIdle(), equalTo(false));
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        
        Thread.sleep(80);
        idle.idle();
        assertThat("State after long processing", idle.isIdle(), equalTo(false));
        
        Thread.sleep(80);
        assertThat("State after time over idle limit", idle.isIdle(), equalTo(true));
        
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        assertThat("State should not be reset once it reached GREEN", idle.isIdle(), equalTo(true));
    }
    
    @Test
    public void testShortIdleTimeButCreateTimeNearCurrentTime() throws InterruptedException {
        assertThat("Initial state", idle.isIdle(), equalTo(false));
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        idle.idle();
        assertThat("State after reset", idle.isIdle(), equalTo(false));
        
        idle.busy(NO_RETRIES, TIME_NOW - 1000);
        assertThat("Create time near TIME_NOW should return idle", idle.isIdle(), equalTo(true));
    }

    /** 
     * In case of blocked queue (retries > MAX_RETRIES) we should report ready
     */
    @Test
    public void testMaxRetries() {
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        idle.idle();
        assertThat("State with no retries", idle.isIdle(), equalTo(false));
        idle.busy(MAX_RETRIES, TEN_MINS_AGO);
        idle.idle();
        assertThat("State with retries <= MAX_RETRIES", idle.isIdle(), equalTo(false));
        idle.busy(MAX_RETRIES + 1, TEN_MINS_AGO);
        idle.idle();
        assertThat("State with retries > MAX_RETRIES", idle.isIdle(), equalTo(true));
        idle.busy(NO_RETRIES, TEN_MINS_AGO);
        assertThat("State should not be reset once it reached idle", idle.isIdle(), equalTo(true));
    }
    
    @Test
    public void testStartIdle() throws InterruptedException {
        assertThat("Initial state", idle.isIdle(), equalTo(false));
        Thread.sleep(IDLE_MILLIES * 2);
        assertThat("State after time over idle limit", idle.isIdle(), equalTo(true));
        idle.close();
    }
    
}
