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

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.queue.QueuedCallback;
import org.osgi.service.event.Event;

import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class PackageQueuedNotifier implements QueuedCallback, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PackageQueuedNotifier.class);

    private final EventAdmin eventAdmin;

    /**
     * (packageId x Future)
     */
    private final Map<String, CompletableFuture<Long>> receiveCallbacks;
    
    public PackageQueuedNotifier(EventAdmin eventAdmin) {
        this.receiveCallbacks = new ConcurrentHashMap<>();
        this.eventAdmin = requireNonNull(eventAdmin);
    }

    private void notifyWait(String pkgId, long offset) {
        CompletableFuture<Long> callback = null;
        if (pkgId != null) {
            callback = receiveCallbacks.remove(pkgId);
        }
        if (callback != null) {
            callback.complete(offset);
        }
    }

    public CompletableFuture<Long> registerWait(String packageId) {
        LOG.debug("Registering wait condition for pkgId={}", packageId);
        CompletableFuture<Long> packageReceived = new CompletableFuture<>();
        receiveCallbacks.put(packageId, packageReceived);
        return packageReceived;
    }

    public void unRegisterWait(String packageId) {
        LOG.debug("Un-registering wait condition for pkgId={}", packageId);
        receiveCallbacks.remove(packageId);
    }

    @Override
    public void queued(List<FullMessage<PackageMessage>> fullMessages) {
        fullMessages.forEach(this::queued);
    }

    private void queued(FullMessage<PackageMessage> fullMessage) {
        long offset = fullMessage.getInfo().getOffset();
        PackageMessage message = fullMessage.getMessage();
        LOG.debug("Queued package {} at offset={}", message, offset);
        sendQueuedEvent(message);
        notifyWait(message.getPkgId(), offset);
    }

    private void sendQueuedEvent(PackageMessage message) {
        Event queuedEvent = DistributionEvent.eventPackageQueued(message, message.getPubAgentName());
        eventAdmin.postEvent(queuedEvent);
    }

    @Override
    public void close() {
        receiveCallbacks.forEach((packageId, callback) -> {
            LOG.debug("Cancel wait condition for distribution package with pkgId={}", packageId);
            callback.cancel(true);
        });
        LOG.info("Package queue notifier closed");
    }
}
