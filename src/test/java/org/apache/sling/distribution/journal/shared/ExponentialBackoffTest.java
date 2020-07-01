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

import static java.time.Duration.of;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class ExponentialBackoffTest {
    private static final int RETRIES = 5;
    private static final long INITIAL_DELAY = of(64, MILLIS).toMillis();
    private static final long MAX_DELAY = of(256, MILLIS).toMillis();
    private static final long LONG_DELAY = of(5, ChronoUnit.SECONDS).toMillis();
    
    private Logger log = LoggerFactory.getLogger(this.getClass());
    
    private volatile CountDownLatch countDown = new CountDownLatch(RETRIES);
    
    @Test
    public void testRetries() throws Exception {
        
        log.info("Checking exponentional backoff");
        ExponentialBackOff backOff = new ExponentialBackOff(INITIAL_DELAY, MAX_DELAY, false, this::checkCallback);
        backOff.startChecks();
        // Run into double trigger protection
        backOff.startChecks();
        boolean finished = this.countDown.await(MAX_DELAY * RETRIES, TimeUnit.MILLISECONDS);
        assertThat("Should finish before the timeout", finished, equalTo(true));

        log.info("Checking for long delay if next error happens quickly");
        this.countDown = new CountDownLatch(1);
        backOff.startChecks();
        boolean finished2 = this.countDown.await(INITIAL_DELAY * 2, TimeUnit.MILLISECONDS);
        assertThat("Should not finish quickly as we called startChecks immediately", finished2, equalTo(false));
        this.countDown.await();

        log.info("Checking for short delay if next error happens after enough time");
        Thread.sleep(MAX_DELAY * 3);
        this.countDown = new CountDownLatch(1);
        backOff.startChecks();
        boolean finished3 = this.countDown.await(INITIAL_DELAY * 4, TimeUnit.MILLISECONDS);
        assertThat("Should finish quickly as we called startChecks after enough delay", finished3, equalTo(true));

        backOff.close();
    }
    
    /**
     * Even with a scheduled check the shutdown is expected to be 
     * fast and to clean up its thread
     */
    @Test(timeout = 500)
    public void testShutdown() throws Exception {
        ExponentialBackOff backoff = new ExponentialBackOff(LONG_DELAY, LONG_DELAY, false, this::checkCallback);
        backoff.startChecks();
        backoff.close(); // We expect this to finish in less than the test timeout
    }
    
    private void checkCallback() {
        this.countDown.countDown();
        if (countDown.getCount() > 0) {
            throw new RuntimeException("Failing num: " + this.countDown.getCount());
        }
    }
}