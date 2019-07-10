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

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExponentialBackoffTest {

    private static final int RETRIES = 10;
    private static final Duration INITIAL_DELAY = Duration.of(64, MILLIS);
    private static final Duration MAX_DELAY = Duration.of(256, MILLIS);
    private CountDownLatch countDown = new CountDownLatch(RETRIES);
    
    @Test
    public void testIsAvailable() throws Exception {
        ExponentialBackOff backOff = new ExponentialBackOff(INITIAL_DELAY, MAX_DELAY, this::checkCallback);
        boolean finished = this.countDown.await(MAX_DELAY.toMillis() * RETRIES, TimeUnit.MILLISECONDS);
        backOff.close();
        assertThat("Should finish before the timeout", finished, equalTo(true));
    }
    
    private void checkCallback() {
        this.countDown.countDown();
        if (countDown.getCount() > 0) {
            throw new RuntimeException("Failing num: " + this.countDown.getCount());
        }
    }
}