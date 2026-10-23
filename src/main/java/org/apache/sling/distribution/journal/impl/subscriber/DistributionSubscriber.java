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

import javax.annotation.ParametersAreNonnullByDefault;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.util.Text;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.ImportPostProcessException;
import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.bookkeeper.BatchEntry;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeper;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperConfig;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperFactory;
import org.apache.sling.distribution.journal.bookkeeper.PackageHandling;
import org.apache.sling.distribution.journal.bookkeeper.SubscriberMetrics;
import org.apache.sling.distribution.journal.impl.precondition.Precondition;
import org.apache.sling.distribution.journal.impl.precondition.Precondition.Decision;
import org.apache.sling.distribution.journal.impl.subscriber.IdleCheck.ReadyReason;
import org.apache.sling.distribution.journal.messages.LogMessage;
import org.apache.sling.distribution.journal.messages.OffsetMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.shared.Delay;
import org.apache.sling.distribution.journal.shared.OnlyOnLeader;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.journal.spi.BatchDistributionPackageBuilder;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.settings.SlingSettingsService;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.util.converter.Converters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.sling.distribution.journal.messages.PackageMessage.ReqType.ADD;
import static org.apache.sling.distribution.journal.messages.PackageMessage.ReqType.DELETE;
import static org.apache.sling.distribution.journal.messages.PackageMessage.ReqType.INVALIDATE;
import static org.apache.sling.distribution.journal.shared.Delay.exponential;
import static org.apache.sling.distribution.journal.shared.Strings.requireNotBlank;

/**
 * A Subscriber SCD agent which consumes messages produced by a
 * {@code DistributionPublisher} agent.
 */
@Component(
        service = {},
        immediate = true,
        property = {"announceDelay=10000"},
        configurationPid = "org.apache.sling.distribution.journal.impl.subscriber.DistributionSubscriberFactory")
@Designate(ocd = SubscriberConfiguration.class, factory = true)
@ParametersAreNonnullByDefault
public class DistributionSubscriber {

    private static final long PRECONDITION_TIMEOUT_MILLIS = SECONDS.toMillis(60);
    static long RETRY_DELAY_MILLIS = SECONDS.toMillis(5);
    static long MAX_RETRY_DELAY_MILLIS = MINUTES.toMillis(15);
    static long QUEUE_FETCH_DELAY_MILLIS = SECONDS.toMillis(1);
    static long BATCH_IMPORTER_STOP_WAIT_MILLIS = SECONDS.toMillis(30);
    private static final Supplier<LongSupplier> catchAllDelays =
            () -> exponential(RETRY_DELAY_MILLIS, MAX_RETRY_DELAY_MILLIS);

    private static final Logger LOG = LoggerFactory.getLogger(DistributionSubscriber.class);

    private final Precondition precondition;

    private final SubscriberMetrics subscriberMetrics;

    private volatile Closeable idleReadyCheck; // NOSONAR

    private volatile IdleCheck idleCheck; // NOSONAR

    private Closeable packagePoller;

    private volatile CommandPoller commandPoller; // NOSONAR

    private BookKeeper bookKeeper;

    private Set<String> queueNames = Collections.emptySet();

    private Announcer announcer;

    private String subAgentName;

    private String pkgType;

    private volatile boolean running = true;

    private LongSupplier catchAllDelay = catchAllDelays.get();

    private final Delay delay = new Delay();

    private int maxBatchSize = 1;

    // Only used when batching is enabled: packages read by the poller, waiting for the batch importer
    private BlockingQueue<QueuedPackage> pendingPackages;

    private Thread batchImporter;

    private AtomicReference<DistributionAgentState> state =
            new AtomicReference<DistributionAgentState>(DistributionAgentState.IDLE);

    @Activate
    public DistributionSubscriber(
            @Reference(name = "packageBuilder") DistributionPackageBuilder packageBuilder,
            @Reference SlingSettingsService slingSettings,
            @Reference MessagingProvider messagingProvider,
            @Reference(name = "precondition") Precondition precondition,
            @Reference MetricsService metricsService,
            @Reference BookKeeperFactory bookKeeperFactory,
            @Reference SubscriberReadyStore subscriberReadyStore,
            @Reference OnlyOnLeader onlyOnLeader,
            SubscriberConfiguration config,
            BundleContext context,
            Map<String, Object> properties) {
        String subSlingId = requireNonNull(slingSettings.getSlingId());
        subAgentName = requireNotBlank(config.name());
        this.precondition = requireNonNull(precondition);
        this.subscriberMetrics =
                new SubscriberMetrics(metricsService, subAgentName, getFirst(config.agentNames()), config.editable());

        if (config.subscriberIdleCheck()) {
            AtomicBoolean readyHolder = subscriberReadyStore.getReadyHolder(subAgentName);
            BiConsumer<ReadyReason, Duration> readyCallback = subscriberMetrics::readinessDuration;
            idleCheck = new SubscriberReady(
                    subAgentName,
                    config.idleMillies(),
                    config.forceReadyMillies(),
                    config.acceptableAgeDiffMs(),
                    readyHolder,
                    System::currentTimeMillis,
                    readyCallback);
            idleReadyCheck = new SubscriberIdleCheck(context, idleCheck, config.subscriberIdleTags());
        } else {
            idleCheck = new NoopIdle();
        }

        queueNames = getNotEmpty(config.agentNames());
        pkgType = requireNonNull(packageBuilder.getType());

        Consumer<PackageStatusMessage> statusSender = messagingProvider.createSender(Topics.STATUS_TOPIC);
        Consumer<LogMessage> logSender = messagingProvider.createSender(Topics.DISCOVERY_TOPIC);

        String packageNodeName = escapeTopicName(messagingProvider.getServerUri(), Topics.PACKAGE_TOPIC);
        String commandNodeName = escapeTopicName(messagingProvider.getServerUri(), Topics.COMMAND_TOPIC);
        BookKeeperConfig bkConfig = new BookKeeperConfig(
                subAgentName,
                subSlingId,
                config.editable(),
                config.maxRetries(),
                config.packageHandling(),
                packageNodeName,
                commandNodeName,
                config.contentPackageExtractorOverwritePrimaryTypesOfFolders());
        bookKeeper =
                bookKeeperFactory.create(packageBuilder, bkConfig, statusSender, logSender, this.subscriberMetrics);

        if (config.editable()) {
            Consumer<Long> clearHandler = (Long offset) -> {
                bookKeeper.storeClearOffset(offset);
                delay.signal();
            };

            commandPoller = new CommandPoller(
                    messagingProvider, subSlingId, subAgentName, bookKeeper.getClearOffset(), clearHandler);
        }

        long startOffset = bookKeeper.loadOffset() + 1;
        String assign = startOffset > 0 ? messagingProvider.assignTo(startOffset) : null;

        maxBatchSize = effectiveBatchSize(config);
        if (maxBatchSize > 1) {
            pendingPackages = new ArrayBlockingQueue<>(maxBatchSize);
        }

        packagePoller = messagingProvider.createPoller(
                Topics.PACKAGE_TOPIC,
                Reset.latest,
                assign,
                HandlerAdapter.create(
                        PackageMessage.class,
                        maxBatchSize > 1 ? this::enqueuePackageMessage : this::handlePackageMessage),
                HandlerAdapter.create(OffsetMessage.class, this::handleOffsetMessage));

        int announceDelay = Converters.standardConverter()
                .convert(properties.get("announceDelay"))
                .defaultValue(10000)
                .to(Integer.class);
        announcer = new Announcer(
                subSlingId,
                subAgentName,
                queueNames,
                messagingProvider.createSender(Topics.DISCOVERY_TOPIC),
                bookKeeper,
                config.maxRetries(),
                config.editable(),
                announceDelay);

        if (maxBatchSize > 1) {
            batchImporter =
                    new Thread(this::runBatchImporter, "Distribution subscriber batch importer " + subAgentName);
            batchImporter.setDaemon(true);
            batchImporter.start();
        }

        LOG.info(
                "Started Subscriber agent={} at offset={}, subscribed to agent names {}, readyCheck={}, maxBatchSize={}",
                subAgentName,
                startOffset,
                queueNames,
                config.subscriberIdleCheck(),
                maxBatchSize);
    }

    private int effectiveBatchSize(SubscriberConfiguration config) {
        int requested = config.maxBatchSize();
        if (requested <= 1) {
            return 1;
        }
        String reason = null;
        if (config.packageHandling() != PackageHandling.Off) {
            reason = "packageHandling is " + config.packageHandling();
        } else if (!bookKeeper.supportsBatchImport()) {
            reason = "the package builder " + pkgType + " does not implement "
                    + BatchDistributionPackageBuilder.class.getName();
        }
        if (reason != null) {
            LOG.warn(
                    "Subscriber agent {} is configured with maxBatchSize={} but imports packages one by one because {}",
                    subAgentName,
                    requested,
                    reason);
            return 1;
        }
        return requested;
    }

    private String getFirst(String[] agentNames) {
        return agentNames != null && agentNames.length > 0 ? agentNames[0] : "";
    }

    public static String escapeTopicName(URI messagingUri, String topicName) {
        return String.format("%s%s_%s", messagingUri.getHost(), escape(messagingUri.getPath()), escape(topicName));
    }

    private static String escape(String st) {
        return Text.escapeIllegalJcrChars(st.replace("/", "_"));
    }

    private Set<String> getNotEmpty(String[] agentNames) {
        return Arrays.stream(agentNames).filter(StringUtils::isNotBlank).collect(toSet());
    }

    @Deactivate
    public void deactivate() {

        /*
         * Note that we don't interrupt blocking calls using Thread.interrupt()
         * because interrupts can stop the Apache Oak repository.
         *
         * See SLING-9340, OAK-2609 and
         * https://jackrabbit.apache.org/oak/docs/dos_and_donts.html
         */

        IOUtils.closeQuietly(announcer, packagePoller, idleReadyCheck, idleCheck, commandPoller);
        running = false;
        awaitBatchImporter();
        LOG.info(
                "Stopped Subscriber agent {}, subscribed to Publisher agent names {} with package builder {}",
                subAgentName,
                queueNames,
                pkgType);
    }

    public DistributionAgentState getState() {
        boolean isBlocked = bookKeeper.getPackageRetries().getSum() > 0;
        return (isBlocked) ? DistributionAgentState.BLOCKED : state.get();
    }

    private void handlePackageMessage(MessageInfo info, PackageMessage message) {
        handlePackageMessage(new QueuedPackage(info, message, shouldSkip(info, message)));
    }

    private void handlePackageMessage(QueuedPackage queued) {
        boolean done = false;
        while (!done && running) {
            done = tryProcess(queued.info, queued.message, queued.skip);
        }
    }

    /**
     * Wait (bounded) for the batch importer to finish the batch it is processing. Packages still
     * waiting in the in-memory queue are dropped. Their offsets have not been stored, so they are
     * read again from the journal when the subscriber starts the next time.
     */
    private void awaitBatchImporter() {
        if (batchImporter == null) {
            return;
        }
        try {
            batchImporter.join(BATCH_IMPORTER_STOP_WAIT_MILLIS);
            if (batchImporter.isAlive()) {
                LOG.warn(
                        "Batch importer of subscriber agent {} is still running after waiting {} ms on deactivation",
                        subAgentName,
                        BATCH_IMPORTER_STOP_WAIT_MILLIS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Called by the poller thread when batching is enabled. Blocks while the queue is full, so the
     * poller reads at most {@code maxBatchSize} packages ahead of the importer.
     */
    private void enqueuePackageMessage(MessageInfo info, PackageMessage message) {
        QueuedPackage queued = new QueuedPackage(info, message, shouldSkip(info, message));
        try {
            while (running) {
                if (pendingPackages.offer(queued, QUEUE_FETCH_DELAY_MILLIS, MILLISECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Importer loop used when batching is enabled. Takes all packages that are already queued, up
     * to {@code maxBatchSize}, so a backlog is imported in batches while single packages are
     * imported without waiting.
     */
    private void runBatchImporter() {
        List<QueuedPackage> batch = new ArrayList<>(maxBatchSize);
        while (running) {
            try {
                QueuedPackage first = pendingPackages.poll(QUEUE_FETCH_DELAY_MILLIS, MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                pendingPackages.drainTo(batch, maxBatchSize - 1);
                processBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // Must not happen as processing retries until success. Stop rather than losing packages.
                LOG.error("Unexpected error in batch importer of subscriber agent {}", subAgentName, e);
                return;
            } finally {
                batch.clear();
            }
        }
    }

    /**
     * Imports batchable packages together across messages that do not match this subscription.
     * Other non-batchable packages end the current run and are then processed on their own.
     */
    private void processBatch(List<QueuedPackage> batch) {
        List<QueuedPackage> run = new ArrayList<>(batch.size());
        for (QueuedPackage queued : batch) {
            if (!running) {
                return;
            }
            if (queued.skip) {
                run.add(queued);
                continue;
            }
            if (isBatchable(queued)) {
                run.add(queued);
                continue;
            }
            importRun(run);
            run.clear();
            handlePackageMessage(queued);
        }
        importRun(run);
    }

    private boolean isBatchable(QueuedPackage queued) {
        PackageMessage.ReqType type = queued.message.getReqType();
        if (type != ADD && type != DELETE) {
            LOG.debug("Package at offset={} is not batchable: request type={}", queued.info.getOffset(), type);
            return false;
        }
        try {
            if (shouldRemove(queued.info.getOffset())) {
                LOG.debug("Package at offset={} is not batchable: should remove", queued.info.getOffset());
                return false;
            }
            return true;
        } catch (PreConditionTimeoutException e) {
            LOG.debug("Package at offset={} is not batchable: pre-condition timeout", queued.info.getOffset());
            return false;
        }
    }

    private void importRun(List<QueuedPackage> run) {
        if (run.isEmpty() || !running) {
            return;
        }
        List<QueuedPackage> imports = new ArrayList<>(run.size());
        List<QueuedPackage> trailingSkips = new ArrayList<>();
        for (QueuedPackage queued : run) {
            if (!queued.skip) {
                imports.add(queued);
                // Committing this import will also cover all skips before it.
                trailingSkips.clear();
            } else {
                trailingSkips.add(queued);
            }
        }
        if (imports.size() > 1 && tryImportBatch(imports)) {
            for (QueuedPackage skipped : trailingSkips) {
                handlePackageMessage(skipped);
            }
            return;
        }
        // Without a successful batch, process everything in journal order.
        for (QueuedPackage queued : run) {
            handlePackageMessage(queued);
        }
    }

    private boolean tryImportBatch(List<QueuedPackage> run) {
        List<BatchEntry> entries = new ArrayList<>(run.size());
        for (QueuedPackage queued : run) {
            MessageInfo info = queued.info;
            entries.add(new BatchEntry(queued.message, info.getOffset(), new Date(info.getCreateTime())));
            subscriberMetrics
                    .getPackageJournalDistributionDuration()
                    .update((currentTimeMillis() - info.getCreateTime()), TimeUnit.MILLISECONDS);
        }
        try {
            this.state.set(DistributionAgentState.RUNNING);
            idleCheck.busy(
                    bookKeeper.getRetries(run.get(0).message.getPubAgentName()),
                    run.get(0).info.getCreateTime());
            blockingSendStoredStatus();
            bookKeeper.importPackages(entries, new Date());
            catchAllDelay = catchAllDelays.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to import batch of {} packages at offsets {}-{}, importing them one by one",
                    run.size(),
                    run.get(0).info.getOffset(),
                    run.get(run.size() - 1).info.getOffset(),
                    e);
            return false;
        } finally {
            idleCheck.idle();
            this.state.set(DistributionAgentState.IDLE);
            announcer.run();
        }
        sendStatusOfBatch();
        return true;
    }

    /**
     * Send the status stored by a successful batch of an editable agent. The batch is committed
     * already, so a failure here must not cause the packages to be imported again. An unsent status
     * is sent before the next package or batch, or after a restart.
     */
    private void sendStatusOfBatch() {
        try {
            blockingSendStoredStatus();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean tryProcess(MessageInfo info, PackageMessage message) {
        return tryProcess(info, message, shouldSkip(info, message));
    }

    private boolean tryProcess(MessageInfo info, PackageMessage message, boolean skip) {
        if (skip) {
            try {
                bookKeeper.skipPackage(info.getOffset());
            } catch (PersistenceException | LoginException e) {
                LOG.warn("Error marking distribution package {} at offset={} as skipped", message, info.getOffset(), e);
            }
            return true;
        }
        subscriberMetrics
                .getPackageJournalDistributionDuration()
                .update((currentTimeMillis() - info.getCreateTime()), TimeUnit.MILLISECONDS);
        try {
            processPackageMessage(info, message);
            catchAllDelay = catchAllDelays.get();
        } catch (PreConditionTimeoutException e) {
            // Precondition timed out. We only log this on info level as it is no error
            LOG.info(e.getMessage());
            delay.await(RETRY_DELAY_MILLIS);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debug(e.getMessage());
        } catch (Exception e) {
            // Catch all to prevent processing from stopping
            LOG.error("Error processing queue item", e);
            delay.await(catchAllDelay.getAsLong());
            return false;
        } finally {
            announcer.run();
        }
        return true;
    }

    private void handleOffsetMessage(MessageInfo info, OffsetMessage message) {
        bookKeeper.handleInitialOffset(info.getOffset());
    }

    private boolean shouldSkip(MessageInfo info, PackageMessage message) {
        if (!queueNames.contains(message.getPubAgentName())) {
            LOG.debug("Skipping distribution package {} at offset={} (not subscribed)", message, info.getOffset());
            return true;
        }
        if (!pkgType.equals(message.getPkgType())) {
            LOG.warn("Skipping distribution package {} at offset={} (bad pkgType)", message, info.getOffset());
            return true;
        }
        return false;
    }

    /**
     * Send status stored in a previous run if exists
     */
    private void blockingSendStoredStatus() throws InterruptedException {
        try (Timer.Context context =
                subscriberMetrics.getSendStoredStatusDuration().time()) {
            int retry = 0;
            while (running) {
                if (bookKeeper.sendStoredStatus(retry)) {
                    return;
                }
                retry++;
            }
        } catch (IOException e) {
            // Ignore .. This is just from timer close
        }

        throw new InterruptedException("Shutting down");
    }

    private void processPackageMessage(MessageInfo info, PackageMessage pkgMsg)
            throws PersistenceException, LoginException, DistributionException, ImportPostProcessException,
                    InterruptedException {
        blockingSendStoredStatus();
        boolean skip = shouldRemove(info.getOffset());
        PackageMessage.ReqType type = pkgMsg.getReqType();
        try {
            this.state.set(DistributionAgentState.RUNNING);
            idleCheck.busy(bookKeeper.getRetries(pkgMsg.getPubAgentName()), info.getCreateTime());
            Date importStartTime = new Date();
            Date createdTime = new Date(info.getCreateTime());
            if (skip) {
                bookKeeper.removePackage(pkgMsg, info.getOffset());
            } else if (type == INVALIDATE) {
                bookKeeper.invalidateCache(pkgMsg, info.getOffset(), createdTime, importStartTime);
            } else {
                bookKeeper.importPackage(pkgMsg, info.getOffset(), createdTime, importStartTime);
            }
            blockingSendStoredStatus();
        } finally {
            idleCheck.idle();
            this.state.set(DistributionAgentState.IDLE);
        }
    }

    private boolean shouldRemove(long offset) {
        return isCleared(offset) || isSkipped(offset);
    }

    private boolean isCleared(long offset) {
        return (commandPoller != null) && commandPoller.isCleared(offset);
    }

    private boolean isSkipped(long offset) {
        return waitPrecondition(offset) == Decision.SKIP;
    }

    private Decision waitPrecondition(long offset) {
        long endTime = System.currentTimeMillis() + PRECONDITION_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < endTime && running) {
            Decision decision = precondition.canProcess(subAgentName, offset);
            if (decision == Decision.WAIT) {
                delay.await(100);
            } else {
                return decision;
            }
        }
        final String msg = String.format(
                "Timeout after %s seconds while waiting to meet the preconditions"
                        + " to import the distribution package at offset %s on topic status",
                MILLISECONDS.toSeconds(PRECONDITION_TIMEOUT_MILLIS), offset);
        throw new PreConditionTimeoutException(msg);
    }

    private static final class QueuedPackage {
        private final MessageInfo info;
        private final PackageMessage message;
        private final boolean skip;

        QueuedPackage(MessageInfo info, PackageMessage message, boolean skip) {
            this.info = info;
            this.message = message;
            this.skip = skip;
        }
    }
}
