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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.sling.distribution.event.DistributionEventTopics;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
        service = {PackageQueuedNotifier.class, EventHandler.class},
        property = EventConstants.EVENT_TOPIC + "=" + DistributionEventTopics.AGENT_PACKAGE_QUEUED
)
public class PackageQueuedNotifier implements EventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PackageQueuedNotifier.class);

    /**
     * (packageId x Future)
     */
    private final Map<String, CompletableFuture<Void>> receiveCallbacks;
    
    public PackageQueuedNotifier() {
        this.receiveCallbacks = new ConcurrentHashMap<>();
    }

    @Deactivate
    public void deactivate() {
        receiveCallbacks.forEach((packageId, callback) -> {
            LOG.debug("Cancel wait condition for package {}", packageId);
            callback.cancel(true);
        });
        LOG.info("Package queue notifier service stopped");
    }

    @Override
    public void handleEvent(Event event) {
        String packageId = (String) event.getProperty(DistributionEvent.PACKAGE_ID);
        LOG.debug("Handling event for packageId {}", packageId);
        CompletableFuture<Void> callback = null;
        if (packageId != null) {
            callback = receiveCallbacks.remove(packageId);
        }
        if (callback != null) {
            callback.complete(null);
        }
    }

    public CompletableFuture<Void> registerWait(String packageId) {
        LOG.debug("Registering wait condition for packageId {}", packageId);
        CompletableFuture<Void> packageReceived = new CompletableFuture<>();
        receiveCallbacks.put(packageId, packageReceived);
        return packageReceived;
    }

    public void unRegisterWait(String packageId) {
        LOG.debug("Un-registering wait condition for packageId {}", packageId);
        receiveCallbacks.remove(packageId);
    }
}
