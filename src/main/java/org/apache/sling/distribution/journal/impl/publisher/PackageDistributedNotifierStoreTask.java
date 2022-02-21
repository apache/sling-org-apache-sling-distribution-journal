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
package org.apache.sling.distribution.journal.impl.publisher;

import javax.annotation.ParametersAreNonnullByDefault;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_IMMEDIATE;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_RUN_ON;
import static org.apache.sling.commons.scheduler.Scheduler.VALUE_RUN_ON_LEADER;

@Component(
        property = {
                PROPERTY_SCHEDULER_CONCURRENT + ":Boolean=false",
                PROPERTY_SCHEDULER_IMMEDIATE + ":Boolean=true",
                PROPERTY_SCHEDULER_PERIOD + ":Long=" + 60, // 1 minute
                PROPERTY_SCHEDULER_RUN_ON + "=" +  VALUE_RUN_ON_LEADER
        })
@ParametersAreNonnullByDefault
public class PackageDistributedNotifierStoreTask implements Runnable {

    /*
     * To avoid conflicting writes, only the leader instance
     * persists the last distributed offset in the repository.
     *
     * The task runs at a frequency of 1 minute to avoid
     * overloading the author repository with a steady stream
     * of fast commits (approximately 10 commit per second).
     */

    @Reference
    private PackageDistributedNotifier notifier;

    @Override
    public void run() {
        notifier.storeLastDistributedOffset();
    }
}
