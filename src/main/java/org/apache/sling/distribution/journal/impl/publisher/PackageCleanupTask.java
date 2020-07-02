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

import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_IMMEDIATE;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_RUN_ON;
import static org.apache.sling.commons.scheduler.Scheduler.VALUE_RUN_ON_LEADER;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.shared.PublisherConfigurationAvailable;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The cleanup task fetches the head offset (the smallest) and
 * the tail offset (the current biggest) on the package topic.
 * The stored packages are then scanned and cleaned if they are
 * no longer referenced by the package topic (offset smaller than
 * the head offset).
 * With clustered deployment, only one Publisher agent should run
 * the package cleanup task on the cluster
 */
@Component(
        property = {
                PROPERTY_SCHEDULER_CONCURRENT + ":Boolean=false",
                PROPERTY_SCHEDULER_IMMEDIATE + ":Boolean=true",
                PROPERTY_SCHEDULER_PERIOD + ":Long=" + 7 * 24 * 60 * 60, // 7 days
                PROPERTY_SCHEDULER_RUN_ON + "=" +  VALUE_RUN_ON_LEADER
        })
@ParametersAreNonnullByDefault
public class PackageCleanupTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(PackageCleanupTask.class);
    private static final long PKG_MAX_LIFETIME_MS = 30 * 24 * 60 * 60 * 1000;

    @Reference
    private PackageRepo packageRepo;

    /**
     * The task runs only when at least one DistributionSubscriber agent is configured.
     */
    @Reference
    private PublisherConfigurationAvailable publisherConfigurationAvailable;

    @Override
    public void run() {
        LOG.info("Starting Package Cleanup Task");
        long deleteOlderThanTime = System.currentTimeMillis() - PKG_MAX_LIFETIME_MS;
        packageRepo.cleanup(deleteOlderThanTime);
        LOG.info("Finished Package Cleanup Task");
    }

}
