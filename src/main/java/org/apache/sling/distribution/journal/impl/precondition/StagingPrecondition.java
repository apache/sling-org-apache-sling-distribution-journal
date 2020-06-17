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

import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;

import java.util.concurrent.TimeoutException;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a precondition that watches status messages from other instances in order to confirm that a package can be processed.
 * The check will block until a status is found. If no status is received in 60 seconds it will throw an exception.
 */
@Component(
        property = {
                "name=staging",
                PROPERTY_SCHEDULER_CONCURRENT + ":Boolean=false",
                PROPERTY_SCHEDULER_PERIOD + ":Long=" + 24 * 60 * 60, // 1 day
        })
public class StagingPrecondition implements Precondition, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(StagingPrecondition.class);

    private static final long STATUS_CHECK_DELAY_MS = 100;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    private volatile PackageStatusWatcher watcher;

    private volatile boolean running = true;
    
    @Activate
    public void activate() {
        watcher = new PackageStatusWatcher(messagingProvider, topics);
        LOG.info("Activated Staging Precondition");
    }

    @Deactivate
    public synchronized void deactivate() {

        /*
         * Note that we don't interrupt blocking calls using Thread.interrupt()
         * because interrupts can stop the Apache Oak repository.
         *
         * See SLING-9340, OAK-2609 and https://jackrabbit.apache.org/oak/docs/dos_and_donts.html
         */

        IOUtils.closeQuietly(watcher);
        running = false;
    }

    @Override
    public boolean canProcess(String subAgentName, long pkgOffset, int timeoutSeconds) throws TimeoutException {
        if (timeoutSeconds < 1) {
            throw new IllegalArgumentException();
        }

        // try to get the status for timeoutSeconds and then throw
        for(int i = 0; running && i < timeoutSeconds * 10 ; i++) {
            Status status = getStatus(subAgentName, pkgOffset);
            if (status != null) {
                return status == Status.IMPORTED;
            } else {
                delayStatusCheck();
            }
        }

        if (!running) {
            throw new IllegalStateException("Staging precondition is shutting down");
        }

        throw new TimeoutException("Timeout waiting for package offset " + pkgOffset + " on status topic.");

    }

    private synchronized Status getStatus(String subAgentName, long pkgOffset) {
        return watcher.getStatus(subAgentName, pkgOffset);
    }
    
    public synchronized void run() {
        LOG.info("Purging StagingPrecondition cache");
        IOUtils.closeQuietly(watcher);
        watcher = new PackageStatusWatcher(messagingProvider, topics);
    }

    private static void delayStatusCheck() {
        try {
            Thread.sleep(STATUS_CHECK_DELAY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
