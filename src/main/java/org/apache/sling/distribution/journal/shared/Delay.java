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

import static java.lang.Math.min;
import static java.util.stream.LongStream.iterate;

public final class Delay {

    private final Object delay = new Object();

    public void await(long delayInMs) {
        synchronized (delay) {
            try {
                delay.wait(delayInMs); //NOSONAR
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void signal() {
        synchronized (delay) {
            delay.notifyAll();
        }
    }

    public static LongSupplier exponential(long startDelay, long maxDelay) {
        return iterate(startDelay, delay -> min(2 * delay, maxDelay)).iterator()::next;
    }

}
