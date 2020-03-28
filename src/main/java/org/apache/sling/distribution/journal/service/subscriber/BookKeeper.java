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
package org.apache.sling.distribution.journal.service.subscriber;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singletonMap;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;
import static org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage.Status.IMPORTED;
import static org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage.Status.REMOVED;
import static org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage.Status.REMOVED_FAILED;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.service.subscriber.SubscriberMetrics.GaugeService;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Keeps track of offset and processed status and manages 
 * coordinates the import/retry handling.
 * 
 * The offset store is identified by the agentName only.
 *
 * With non clustered publish instances deployment, each
 * instance stores the offset in its own node store, thus
 * avoiding mix ups. Moreover, when cloning an instance
 * from a node store, the cloned instance will implicitly
 * recover the offsets and start from the last processed
 * offset.
 *
 * With clustered publish instances deployment, only one
 * Subscriber agent must run on the cluster in order to
 * avoid mix ups.
 *
 * The clustered and non clustered publish instances use
 * cases can be supported by only running the Subscriber
 * agent on the leader instance.
 */
public class BookKeeper implements Closeable {
    private static final String KEY_OFFSET = "offset";
    private static final String SUBSERVICE_IMPORTER = "importer";
    private static final String SUBSERVICE_BOOKKEEPER = "bookkeeper";
    private static final int RETRY_SEND_DELAY = 1000;
    private static final int COMMIT_AFTER_NUM_SKIPPED = 10;

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final ResourceResolverFactory resolverFactory;
    private final SubscriberMetrics subscriberMetrics;
    private final PackageHandler packageHandler;
    public final SubscriberIdle subscriberIdle;
    private final EventAdmin eventAdmin;
    
    private final Consumer<PackageStatusMessage> sender;
    private final boolean editable;
    private final int maxRetries;
    private final boolean errorQueueEnabled;

    private final PackageRetries packageRetries = new PackageRetries();
    private final LocalStore statusStore;
    private final LocalStore processedOffsets;
    private final String subAgentName;
    private final String subSlingId;
    private GaugeService<Integer> retriesGauge;
    private int skippedCounter = 0;

    BookKeeper(ResourceResolverFactory resolverFactory, 
            SubscriberMetrics subscriberMetrics,
            PackageHandler packageHandler,
            SubscriberIdle subscriberIdle,
            EventAdmin eventAdmin,
            Consumer<PackageStatusMessage> sender,
            String subAgentName,
            String subSlingId,
            boolean editable, 
            int maxRetries) { 
        this.packageHandler = packageHandler;
        this.subscriberIdle = subscriberIdle;
        this.eventAdmin = eventAdmin;
        String nameRetries = SubscriberMetrics.SUB_COMPONENT + ".current_retries;sub_name=" + subAgentName;
        this.subscriberMetrics = subscriberMetrics;
        this.retriesGauge = subscriberMetrics.createGauge(nameRetries, "Retries of current package", packageRetries::getSum);
        this.resolverFactory = resolverFactory;
        this.sender = sender;
        this.subAgentName = subAgentName;
        this.subSlingId = subSlingId;
        this.editable = editable;
        this.maxRetries = maxRetries;
        // Error queues are enabled when the number
        // of retry attempts is limited ; disabled otherwise
        this.errorQueueEnabled = (maxRetries >= 0);
        this.statusStore = new LocalStore(resolverFactory, "statuses", subAgentName);
        this.processedOffsets = new LocalStore(resolverFactory, "packages", subAgentName);
    }
    
    public void processPackage(MessageInfo info, PackageMessage pkgMsg, BiPredicate<Long, PackageMessage> shouldRemove) throws Exception {
        try {
            sendStoredStatus();
            long offset = info.getOffset();
            boolean remove = shouldRemove.test(offset, pkgMsg);
            subscriberIdle.busy();
            if (remove) {
                removePackage(pkgMsg, offset);
            } else {
                long createdTime = info.getCreateTime();
                importPackage(pkgMsg, offset, createdTime);
            }
        } finally {
            subscriberIdle.idle();
            sendStoredStatus();
        }
    }
    
    public DistributionAgentState getState() {
        if (subscriberIdle.isIdle()) {
            return DistributionAgentState.IDLE;
        }
        return packageRetries.getSum() > 0 ? DistributionAgentState.BLOCKED : DistributionAgentState.RUNNING;
    }

    /**
     * We aim at processing the packages exactly once. Processing the packages
     * exactly once is possible with the following conditions
     *
     * I. The package importer is configured to disable auto-committing changes.
     *
     * II. A single commit aggregates three content updates
     *
     * C1. install the package 
     * C2. store the processing status 
     * C3. store the offset processed
     *
     * Some package importers require auto-saving or issue partial commits before
     * failing. For those packages importers, we aim at processing packages at least
     * once, thanks to the order in which the content updates are applied.
     */
    private void importPackage(PackageMessage pkgMsg, long offset, long createdTime) throws DistributionException {
        log.info("Importing distribution package {} of type {} at offset {}", 
                pkgMsg.getPkgId(), pkgMsg.getReqType(), offset);
        addPackageMDC(pkgMsg);
        try (Timer.Context context = subscriberMetrics.getImportedPackageDuration().time();
                ResourceResolver importerResolver = getServiceResolver(SUBSERVICE_IMPORTER)) {
            packageHandler.apply(importerResolver, pkgMsg);
            if (editable) {
                storeStatus(importerResolver, new PackageStatus(IMPORTED, offset, pkgMsg.getPubAgentName()));
            }
            storeOffset(importerResolver, offset);
            importerResolver.commit();
            subscriberMetrics.getImportedPackageSize().update(pkgMsg.getPkgLength());
            subscriberMetrics.getPackageDistributedDuration().update((currentTimeMillis() - createdTime), TimeUnit.MILLISECONDS);
            packageRetries.clear(pkgMsg.getPubAgentName());
            Event event = ImportedEventFactory.create(pkgMsg, subAgentName);
            eventAdmin.postEvent(event);
        } catch (LoginException | IOException | RuntimeException e) {
            failure(pkgMsg, offset, e);
        } finally {
            MDC.clear();
        }
    }
    
    private void addPackageMDC(PackageMessage pkgMsg) {
        MDC.put("module", "distribution");
        MDC.put("package-id", pkgMsg.getPkgId());
        String paths = pkgMsg.getPathsList().stream().collect(Collectors.joining(","));
        MDC.put("paths", paths);
        MDC.put("pub-sling-id", pkgMsg.getPubSlingId());
        String pubAgentName = pkgMsg.getPubAgentName();
        MDC.put("pub-agent-name", pubAgentName);
        MDC.put("distribution-message-type", pkgMsg.getReqType().name());
        MDC.put("retries", Integer.toString(packageRetries.get(pubAgentName)));
        MDC.put("sub-sling-id", subSlingId);
        MDC.put("sub-agent-name", subAgentName);
    }
    
    /**
     * Should be called on a exception while importing a package.
     * 
     * When we use an error queue and the max retries is reached the package is removed.
     * In all other cases a DistributionException is thrown that signals that we should retry the
     * package.
     * 
     * @param pkgMsg
     * @param offset
     * @param e
     * @throws DistributionException if the package should be retried
     */
    private void failure(PackageMessage pkgMsg, long offset, Exception e) throws DistributionException {
        subscriberMetrics.getFailedPackageImports().mark();

        String pubAgentName = pkgMsg.getPubAgentName();
        int retries = packageRetries.get(pubAgentName);
        if (errorQueueEnabled && retries >= maxRetries) {
            log.warn("Failed to import distribution package {} at offset {} after {} retries, removing the package.", 
                    pkgMsg.getPkgId(), offset, retries);
            removeFailedPackage(pkgMsg, offset);
        } else {
            packageRetries.increase(pubAgentName);
            String msg = format("Error processing distribution package %s. Retry attempts %s/%s.", pkgMsg.getPkgId(), retries, errorQueueEnabled ? Integer.toString(maxRetries) : "infinite");
            throw new DistributionException(msg, e);
        }
    }

    private void removePackage(PackageMessage pkgMsg, long offset) throws LoginException, PersistenceException {
        log.info("Removing distribution package {} of type {} at offset {}", 
                pkgMsg.getPkgId(), pkgMsg.getReqType(), offset);
        Timer.Context context = subscriberMetrics.getRemovedPackageDuration().time();
        try (ResourceResolver resolver = getServiceResolver(SUBSERVICE_BOOKKEEPER)) {
            if (editable) {
                storeStatus(resolver, new PackageStatus(REMOVED, offset, pkgMsg.getPubAgentName()));
            }
            storeOffset(resolver, offset);
            resolver.commit();
        }
        packageRetries.clear(pkgMsg.getPubAgentName());
        context.stop();
    }
    
    public void skipPackage(long offset) {
        log.info("Skipping package at offset {}", offset);
        if (shouldCommitSkipped()) {
            try (ResourceResolver resolver = getServiceResolver(SUBSERVICE_BOOKKEEPER)) {
                storeOffset(resolver, offset);
                resolver.commit();
            } catch (Exception e) {
                log.warn("");
            }
        }
    }

    private synchronized boolean shouldCommitSkipped() {
        skippedCounter ++;
        if (skippedCounter > COMMIT_AFTER_NUM_SKIPPED) {
            skippedCounter = 1;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Send status stored in a previous run if exists
     * @throws InterruptedException
     */
    private void sendStoredStatus() throws InterruptedException {
        try (Timer.Context context = subscriberMetrics.getSendStoredStatusDuration().time()) {
            PackageStatus status = new PackageStatus(statusStore.load());
            boolean sent = status.sent;
            int retry = 0;
            while (!sent) {
                sent = sendStoredStatusOnce(status, retry++);
            }
        } catch (IOException e) {
            log.warn("Error in timer close", e);
        }
    }

    private boolean sendStoredStatusOnce(PackageStatus status, int retry) throws InterruptedException {
        try {
            sendStatusMessage(status);
            markStatusSent();
            return true;
        } catch (Exception e) {
            log.warn("Cannot send status (retry {})", retry, e);
            Thread.sleep(RETRY_SEND_DELAY);
            return false;
        }
    }
    
    private void sendStatusMessage(PackageStatus status) {
        PackageStatusMessage pkgStatMsg = PackageStatusMessage.newBuilder()
                .setSubSlingId(subSlingId)
                .setSubAgentName(subAgentName)
                .setPubAgentName(status.pubAgentName)
                .setOffset(status.offset)
                .setStatus(status.status)
                .build();
        sender.accept(pkgStatMsg);
        log.info("Sent status message {}",  pkgStatMsg);
    }

    private void markStatusSent() {
        try (ResourceResolver resolver = getServiceResolver(SUBSERVICE_BOOKKEEPER)) {
            statusStore.store(resolver, "sent", true);
            resolver.commit();
        } catch (Exception e) {
            log.warn("Failed to mark status as sent", e);
        }
    }
    
    public long loadOffset() {
        return  processedOffsets.load(KEY_OFFSET, -1L);
    }

    public int getRetries(String pubAgentName) {
        return packageRetries.get(pubAgentName);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(retriesGauge);
    }
    
    private void removeFailedPackage(PackageMessage pkgMsg, long offset) throws DistributionException {
        log.info("Removing failed distribution package {} of type {} at offset {}", 
                pkgMsg.getPkgId(), pkgMsg.getReqType(), offset);
        Timer.Context context = subscriberMetrics.getRemovedFailedPackageDuration().time();
        try (ResourceResolver resolver = getServiceResolver(SUBSERVICE_BOOKKEEPER)) {
            storeStatus(resolver, new PackageStatus(REMOVED_FAILED, offset, pkgMsg.getPubAgentName()));
            storeOffset(resolver, offset);
            resolver.commit();
        } catch (Exception e) {
            throw new DistributionException("Error removing failed package", e);
        }
        context.stop();
    }

    private void storeStatus(ResourceResolver resolver, PackageStatus packageStatus) throws PersistenceException {
        Map<String, Object> statusMap = packageStatus.asMap();
        statusStore.store(resolver, statusMap);
        log.info("Stored status {}", statusMap);
    }

    private void storeOffset(ResourceResolver resolver, long offset) throws PersistenceException {
        processedOffsets.store(resolver, KEY_OFFSET, offset);
    }

    private ResourceResolver getServiceResolver(String subService) throws LoginException {
        return resolverFactory.getServiceResourceResolver(singletonMap(SUBSERVICE, subService));
    }

    class PackageStatus {
        final Status status;
        final Long offset;
        final String pubAgentName;
        final Boolean sent;

        PackageStatus(Status status, long offset, String pubAgentName) {
            this.status = status;
            this.offset = offset;
            this.pubAgentName = pubAgentName;
            this.sent = false;
        }
        
        PackageStatus(ValueMap statusMap) {
            Integer statusNum = statusMap.get("statusNumber", Integer.class);
            this.status = statusNum !=null ? Status.valueOf(statusNum) : null;
            this.offset = statusMap.get(KEY_OFFSET, Long.class);
            this.pubAgentName = statusMap.get("pubAgentName", String.class);
            this.sent = statusMap.get("sent", true);
        }

        Map<String, Object> asMap() {
            Map<String, Object> s = new HashMap<>();
            s.put("pubAgentName", pubAgentName);
            s.put("statusNumber", status.getNumber());
            s.put(KEY_OFFSET, offset);
            s.put("sent", sent);
            return s;
        }
    }
}
