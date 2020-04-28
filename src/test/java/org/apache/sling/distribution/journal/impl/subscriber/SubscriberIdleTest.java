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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.felix.systemready.CheckStatus.State;
import org.junit.Test;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;

public class SubscriberIdleTest {

    private static final int IDLE_MILLIES = 40;
    private SubscriberIdle idle;
    private AtomicBoolean readyHolder;
    
    @Test
    public void testIdle() throws InterruptedException {
        BundleContext context = Mockito.mock(BundleContext.class);
        readyHolder = new AtomicBoolean();
        idle = new SubscriberIdle(context , IDLE_MILLIES, readyHolder);
        assertState("Initial state", State.RED);
        idle.busy();
        idle.idle();
        assertState("State after reset", State.RED);
        Thread.sleep(30);
        assertState("State after time below idle limit", State.RED);
        idle.busy();
        Thread.sleep(80);
        idle.idle();
        assertState("State after long processing", State.RED);
        Thread.sleep(80);
        assertState("State after time over idle limit", State.GREEN);
        idle.busy();
        assertState("State should not be reset once it reached GREEN", State.GREEN);
        idle.close();
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
