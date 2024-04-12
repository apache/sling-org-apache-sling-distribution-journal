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

import java.util.concurrent.Callable;

import org.apache.sling.commons.metrics.Timer;

public final class Timed {
    
    private Timed() {
    }
    
    /**
     * Runs provided code updating provided metric
     * with its execution time.
     * The method guarantees that the metric is updated
     * even if the code throws an exception
     * @param metric metric to update
     * @param code code to clock
     * @throws Exception actually it doesn't
     */
    public static void timed(Timer metric, Runnable code) throws Exception {
        try (Timer.Context ignored = metric.time()) {
            code.run();
        }
    }

    /**
     * Runs provided code updating provided metric
     * with its execution time.
     * The method guarantees that the metric is updated
     * even if the code throws an exception
     * @param metric metric to update
     * @param code code to clock
     * @return a value returned but <code>code.call()</code> invocation
     * @throws Exception if underlying code throws
     */
    public static <T> T timed(Timer metric, Callable<T> code) throws Exception {
        try (Timer.Context ignored = metric.time()) {
            return code.call();
        }
    }
}
