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

import java.util.function.LongSupplier;
import java.util.stream.LongStream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DelaysTest {

    private static final long START_DELAY = 1L;

    private static final long MAX_DELAY = 1000L;

    @Test
    public void testExponentialStartDelay() {
        LongSupplier delay = Delays.exponential(START_DELAY, MAX_DELAY);
        assertEquals(START_DELAY, delay.getAsLong());
    }

    @Test
    public void testExponentialIncreasingDelay() {
        LongSupplier delay = Delays.exponential(START_DELAY, MAX_DELAY);
        assertTrue(delay.getAsLong() < delay.getAsLong());
    }

    @Test
    public void testExponentialIncreasingRateDelay() {
        LongSupplier delay = Delays.exponential(START_DELAY, MAX_DELAY);
        assertEquals(1, delay.getAsLong());
        assertEquals(2, delay.getAsLong());
        assertEquals(4, delay.getAsLong());
    }

    @Test
    public void testExponentialMaxDelay() {
        LongSupplier delay = Delays.exponential(START_DELAY, MAX_DELAY);
        long maxAfterHundredDelays = LongStream.generate(delay).limit(100).max().orElseThrow(IllegalStateException::new);
        assertEquals(MAX_DELAY, maxAfterHundredDelays);
    }

}