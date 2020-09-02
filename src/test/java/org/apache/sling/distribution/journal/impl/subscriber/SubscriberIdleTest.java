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
import static org.junit.Assert.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.felix.systemready.CheckStatus.State;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;

public class SubscriberIdleTest {

    private static final int IDLE_MILLIES = 40;
    private SubscriberIdle idle;
    private AtomicBoolean readyHolder;

    @Before
    public void before() {
        BundleContext context = Mockito.mock(BundleContext.class);
        readyHolder = new AtomicBoolean();
        idle = new SubscriberIdle(context , IDLE_MILLIES, readyHolder);
    }

    @After
    public void after() {
        idle.close();
    }
    
    @Test
    public void testIdle() throws InterruptedException {
        assertState("Initial state", State.RED);
        idle.busy(0);
        idle.idle();
        assertState("State after reset", State.RED);
        Thread.sleep(30);
        assertState("State after time below idle limit", State.RED);
        idle.busy(0);
        Thread.sleep(80);
        idle.idle();
        assertState("State after long processing", State.RED);
        Thread.sleep(80);
        assertState("State after time over idle limit", State.GREEN);
        idle.busy(0);
        assertState("State should not be reset once it reached GREEN", State.GREEN);
    }

    @Test
    public void testMaxRetries() {
        idle.busy(0);
        idle.idle();
        assertState("State with no retries", State.RED);
        idle.busy(MAX_RETRIES);
        idle.idle();
        assertState("State with retries <= MAX_RETRIES", State.RED);
        idle.busy(MAX_RETRIES + 1);
        idle.idle();
        assertState("State with retries > MAX_RETRIES", State.GREEN);
        idle.busy(0);
        assertState("State should not be reset once it reached GREEN", State.GREEN);
    }
    
    @Test
    public void testStartIdle() throws InterruptedException {
        BundleContext context = Mockito.mock(BundleContext.class);
        readyHolder = new AtomicBoolean();
        idle = new SubscriberIdle(context , IDLE_MILLIES, readyHolder);
        assertState("Initial state", State.RED);
        Thread.sleep(IDLE_MILLIES * 2);
        assertState("State after time over idle limit", State.GREEN);
        idle.close();
    }

    private void assertState(String message, State expectedState) {
        assertThat(message, idle.getStatus().getState(), equalTo(expectedState));
    }
    
}
