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

/**
 * Periodical task to cleanup the resources
 * used by the cache.
 */
@Component(
        service = Runnable.class,
        property = {
                PROPERTY_SCHEDULER_CONCURRENT + ":Boolean=false",
                PROPERTY_SCHEDULER_PERIOD + ":Long=" + 12 * 60 * 60 // 12 hours
        })
@ParametersAreNonnullByDefault
public class QueueCacheCleanupTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(QueueCacheCleanupTask.class);

    @Reference
    private PubQueueCacheService queueCacheService;

    @Override
    public void run() {
        LOG.info("Starting package cache cleanup task");
        queueCacheService.cleanup();
        LOG.info("Stopping package cache cleanup task");
    }
}

