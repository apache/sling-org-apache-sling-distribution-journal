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
package org.apache.sling.distribution.journal.bookkeeper;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singletonMap;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.LocalStore;
import org.apache.sling.distribution.journal.shared.PackageRetries;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService.GaugeService;
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
    public static final String STORE_TYPE_PACKAGE = "packages";
    public static final String STORE_TYPE_STATUS = "statuses";
    public static final String KEY_OFFSET = "offset";
    public static final int COMMIT_AFTER_NUM_SKIPPED = 10;
    private static final String SUBSERVICE_IMPORTER = "importer";
    private static final String SUBSERVICE_BOOKKEEPER = "bookkeeper";
    private static final int RETRY_SEND_DELAY = 1000;

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final ResourceResolverFactory resolverFactory;
    private final DistributionMetricsService distributionMetricsService;
    private final PackageHandler packageHandler;
    private final EventAdmin eventAdmin;
    private final Consumer<PackageStatusMessage> sender;
    private final BookKeeperConfig config;
    private final boolean errorQueueEnabled;

    private final PackageRetries packageRetries = new PackageRetries();
    private final LocalStore statusStore;
    private final LocalStore processedOffsets;
    private final GaugeService<Integer> retriesGauge;
    private int skippedCounter = 0;

    public BookKeeper(
            ResourceResolverFactory resolverFactory, 
            DistributionMetricsService distributionMetricsService,
            PackageHandler packageHandler,
            EventAdmin eventAdmin,
            Consumer<PackageStatusMessage> sender,
            BookKeeperConfig config) { 
        this.packageHandler = packageHandler;
        this.eventAdmin = eventAdmin;
        this.sender = sender;
        this.config = config;
        String nameRetries = DistributionMetricsService.SUB_COMPONENT + ".current_retries;sub_name=" + config.getSubAgentName();
        this.retriesGauge = distributionMetricsService.createGauge(nameRetries, "Retries of current package", packageRetries::getSum);
        this.resolverFactory = resolverFactory;
        this.distributionMetricsService = distributionMetricsService;
        // Error queues are enabled when the number
        // of retry attempts is limited ; disabled otherwise
        this.errorQueueEnabled = (config.getMaxRetries() >= 0);
        this.statusStore = new LocalStore(resolverFactory, STORE_TYPE_STATUS, config.getSubAgentName());
        this.processedOffsets = new LocalStore(resolverFactory, STORE_TYPE_PACKAGE, config.getSubAgentName());
        log.info("Started bookkeeper {}.", config);
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
    public void importPackage(PackageMessage pkgMsg, long offset, long createdTime) throws DistributionException {
        log.info("Importing distribution package {} of type {} at offset {}", 
                pkgMsg.getPkgId(), pkgMsg.getReqType(), offset);
        addPackageMDC(pkgMsg);
        try (Timer.Context context = distributionMetricsService.getImportedPackageDuration().time();
                ResourceResolver importerResolver = getServiceResolver(SUBSERVICE_IMPORTER)) {
            packageHandler.apply(importerResolver, pkgMsg);
            if (config.isEditable()) {
                storeStatus(importerResolver, new PackageStatus(PackageStatusMessage.Status.IMPORTED, offset, pkgMsg.getPubAgentName()));
            }
            storeOffset(importerResolver, offset);
            importerResolver.commit();
            distributionMetricsService.getImportedPackageSize().update(pkgMsg.getPkgLength());
            distributionMetricsService.getPackageDistributedDuration().update((currentTimeMillis() - createdTime), TimeUnit.MILLISECONDS);
            packageRetries.clear(pkgMsg.getPubAgentName());
            Event event = new ImportedEvent(pkgMsg, config.getSubAgentName()).toEvent();
            eventAdmin.postEvent(event);
        } catch (DistributionException | LoginException | IOException | RuntimeException e) {
            failure(pkgMsg, offset, e);
        } finally {
            MDC.clear();
        }
    }
    
    private void addPackageMDC(PackageMessage pkgMsg) {
        MDC.put("module", "distribution");
        MDC.put("package-id", pkgMsg.getPkgId());
        String paths = String.join(",", pkgMsg.getPaths());
        MDC.put("paths", paths);
        MDC.put("pub-sling-id", pkgMsg.getPubSlingId());
        String pubAgentName = pkgMsg.getPubAgentName();
        MDC.put("pub-agent-name", pubAgentName);
        MDC.put("distribution-message-type", pkgMsg.getReqType().name());
        MDC.put("retries", Integer.toString(packageRetries.get(pubAgentName)));
        MDC.put("sub-sling-id", config.getSubSlingId());
        MDC.put("sub-agent-name", config.getSubAgentName());
    }
    
    /**
     * Should be called on a exception while importing a package.
     * 
     * When we use an error queue and the max retries is reached the package is removed.
     * In all other cases a DistributionException is thrown that signals that we should retry the
     * package.
     *
     * @throws DistributionException if the package should be retried
     */
    private void failure(PackageMessage pkgMsg, long offset, Exception e) throws DistributionException {
        distributionMetricsService.getFailedPackageImports().mark();

        String pubAgentName = pkgMsg.getPubAgentName();
        int retries = packageRetries.get(pubAgentName);
        if (errorQueueEnabled && retries >= config.getMaxRetries()) {
            log.warn("Failed to import distribution package {} at offset {} after {} retries, removing the package.", 
                    pkgMsg.getPkgId(), offset, retries);
            removeFailedPackage(pkgMsg, offset);
        } else {
            packageRetries.increase(pubAgentName);
            String retriesSt = errorQueueEnabled ? Integer.toString(config.getMaxRetries()) : "infinite";
            String msg = format("Error processing distribution package %s. Retry attempts %s/%s.", pkgMsg.getPkgId(), retries, retriesSt);
            throw new DistributionException(msg, e);
        }
    }

    public void removePackage(PackageMessage pkgMsg, long offset) throws LoginException, PersistenceException {
        log.info("Removing distribution package {} of type {} at offset {}", 
                pkgMsg.getPkgId(), pkgMsg.getReqType(), offset);
        Timer.Context context = distributionMetricsService.getRemovedPackageDuration().time();
        try (ResourceResolver resolver = getServiceResolver(SUBSERVICE_BOOKKEEPER)) {
            if (config.isEditable()) {
                storeStatus(resolver, new PackageStatus(Status.REMOVED, offset, pkgMsg.getPubAgentName()));
            }
            storeOffset(resolver, offset);
            resolver.commit();
        }
        packageRetries.clear(pkgMsg.getPubAgentName());
        context.stop();
    }
    
    public void skipPackage(long offset) throws LoginException, PersistenceException {
        log.info("Skipping package at offset {}", offset);
        if (shouldCommitSkipped()) {
            try (ResourceResolver resolver = getServiceResolver(SUBSERVICE_BOOKKEEPER)) {
                storeOffset(resolver, offset);
                resolver.commit();
            }
        }
    }

    public synchronized boolean shouldCommitSkipped() {
        skippedCounter ++;
        if (skippedCounter > COMMIT_AFTER_NUM_SKIPPED) {
            skippedCounter = 1;
            return true;
        } else {
            return false;
        }
    }

    /**
     * @return {@code true} if the status has been sent ;
     *         {@code false} otherwise.
     */
    public boolean sendStoredStatus(int retry) {
        PackageStatus status = new PackageStatus(statusStore.load());
        return status.sent || sendStoredStatus(status, retry);
    }

    private boolean sendStoredStatus(PackageStatus status, int retry) {
        try {
            sendStatusMessage(status);
            markStatusSent();
            return true;
        } catch (Exception e) {
            log.warn("Cannot send status (retry {})", retry, e);
            retryDelay();
            return false;
        }
    }
    
    private void sendStatusMessage(PackageStatus status) {
        PackageStatusMessage pkgStatMsg = PackageStatusMessage.builder()
                .subSlingId(config.getSubSlingId())
                .subAgentName(config.getSubAgentName())
                .pubAgentName(status.pubAgentName)
                .offset(status.offset)
                .status(status.status)
                .build();
        sender.accept(pkgStatMsg);
        log.info("Sent status message {}",  pkgStatMsg);
    }

    public void markStatusSent() {
        try (ResourceResolver resolver = getServiceResolver(SUBSERVICE_BOOKKEEPER)) {
            statusStore.store(resolver, "sent", true);
            resolver.commit();
        } catch (Exception e) {
            log.warn("Failed to mark status as sent", e);
        }
    }
    
    public long loadOffset() {
        return processedOffsets.load(KEY_OFFSET, -1L);
    }

    public int getRetries(String pubAgentName) {
        return packageRetries.get(pubAgentName);
    }

    public PackageRetries getPackageRetries() {
        return packageRetries;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(retriesGauge);
    }
    
    private void removeFailedPackage(PackageMessage pkgMsg, long offset) throws DistributionException {
        log.info("Removing failed distribution package {} of type {} at offset {}", 
                pkgMsg.getPkgId(), pkgMsg.getReqType(), offset);
        Timer.Context context = distributionMetricsService.getRemovedFailedPackageDuration().time();
        try (ResourceResolver resolver = getServiceResolver(SUBSERVICE_BOOKKEEPER)) {
            storeStatus(resolver, new PackageStatus(Status.REMOVED_FAILED, offset, pkgMsg.getPubAgentName()));
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

    static void retryDelay() {
        try {
            Thread.sleep(RETRY_SEND_DELAY);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static class PackageStatus {
        public final Status status;
        final Long offset;
        final String pubAgentName;
        final Boolean sent;

        PackageStatus(Status status, long offset, String pubAgentName) {
            this.status = status;
            this.offset = offset;
            this.pubAgentName = pubAgentName;
            this.sent = false;
        }
        
        public PackageStatus(ValueMap statusMap) {
            Integer statusNum = statusMap.get("statusNumber", Integer.class);
            this.status = statusNum !=null ? Status.fromNumber(statusNum) : null;
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
