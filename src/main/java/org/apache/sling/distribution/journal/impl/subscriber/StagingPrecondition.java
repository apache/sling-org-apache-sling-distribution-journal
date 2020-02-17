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
package org.apache.sling.distribution.journal.impl.subscriber;

import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage.Status;
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
                Precondition.PROPERTY_NAME + "=staging",
                PROPERTY_SCHEDULER_CONCURRENT + ":Boolean=false",
                PROPERTY_SCHEDULER_PERIOD + ":Long=" + 24 * 60 * 60, // 1 day
        })
public class StagingPrecondition implements Precondition, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(StagingPrecondition.class);

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
    public void deactivate() {
        IOUtils.closeQuietly(watcher);
        running = false;
    }

    @Override
    public synchronized boolean canProcess(String subAgentName, long pkgOffset, int timeoutSeconds) throws InterruptedException {
        if (timeoutSeconds < 1) {
            throw new IllegalArgumentException();
        }

        // try to get the status for timeoutSeconds and then throw
        for(int i=0; i < timeoutSeconds * 10; i++) {
            Status status = watcher.getStatus(subAgentName, pkgOffset);

            if (status != null) {
                return status == Status.IMPORTED;
            } else {
                this.wait(100);
            }
            
            if (!running) {
                throw new InterruptedException("Staging precondition is shutting down");
            }
        }

        throw new IllegalStateException("Timeout waiting for package offset " + pkgOffset + " on status topic.");
    }
    
    public synchronized void run() {
        LOG.info("Purging StagingPrecondition cache");
        IOUtils.closeQuietly(watcher);
        watcher = new PackageStatusWatcher(messagingProvider, topics);
    }

}
