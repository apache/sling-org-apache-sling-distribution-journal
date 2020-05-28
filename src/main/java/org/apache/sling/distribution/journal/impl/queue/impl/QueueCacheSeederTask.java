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
package org.apache.sling.distribution.journal.impl.queue.impl;

import javax.annotation.ParametersAreNonnullByDefault;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_RUN_ON;
import static org.apache.sling.commons.scheduler.Scheduler.VALUE_RUN_ON_LEADER;

/**
 * Periodical task to persist a cache seed
 * to the repository. The task must run only
 * on the leader instance to avoid concurrent
 * writes and reduce write operations in
 * clustered deployments.
 */
@Component(
        service = Runnable.class,
        property = {
                PROPERTY_SCHEDULER_CONCURRENT + ":Boolean=false",
                PROPERTY_SCHEDULER_RUN_ON + "=" +  VALUE_RUN_ON_LEADER,
                PROPERTY_SCHEDULER_PERIOD + ":Long=" + 15 * 60 // 15 minutes
        })
@ParametersAreNonnullByDefault
public class QueueCacheSeederTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(QueueCacheSeederTask.class);

    @Reference
    private PubQueueCacheService queueCacheService;

    @Override
    public void run() {
        LOG.debug("Starting package cache seeder task");
        queueCacheService.storeSeed();
        LOG.debug("Stopping package cache seeder task");
    }
}
