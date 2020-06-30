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
package org.apache.sling.distribution.journal.impl.precondition;

import java.util.concurrent.TimeoutException;

/**
 * Extension point for checking if a package can be processed by a subscriber.
 */
public interface Precondition {

    /**
     * Checks if a package can be processed
     * @param pkgOffset the offset of the package
     * @param timeoutSeconds max seconds to wait until returning
     * @throws TimeoutException if the timeout expired without being able to determine status
     * @throws InterruptedException if the thread was interrupted and should shut down
     * @return true if the package can be processed; otherwise it returns false.
     */
    Decision canProcess(String subAgentName, long pkgOffset);

    enum Decision { ACCEPT, SKIP, WAIT}
}
