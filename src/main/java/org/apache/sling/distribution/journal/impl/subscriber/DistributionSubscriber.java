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

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;
import static org.apache.sling.distribution.journal.messages.PackageMessage.ReqType.INVALIDATE;
import static org.apache.sling.distribution.journal.shared.Delay.exponential;
import static org.apache.sling.distribution.journal.shared.Strings.requireNotBlank;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.util.Text;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.ImportPostProcessException;
import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeper;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperConfig;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperFactory;
import org.apache.sling.distribution.journal.impl.precondition.Precondition;
import org.apache.sling.distribution.journal.impl.precondition.Precondition.Decision;
import org.apache.sling.distribution.journal.messages.LogMessage;
import org.apache.sling.distribution.journal.messages.OffsetMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.shared.Delay;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.Topics;
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

/**
 * A Subscriber SCD agent which consumes messages produced by a
 * {@code DistributionPublisher} agent.
 */
@Component(service = {}, immediate = true, property = {
        "announceDelay=10000" }, configurationPid = "org.apache.sling.distribution.journal.impl.subscriber.DistributionSubscriberFactory")
@Designate(ocd = SubscriberConfiguration.class, factory = true)
@ParametersAreNonnullByDefault
public class DistributionSubscriber {

    private static final long PRECONDITION_TIMEOUT = SECONDS.toMillis(60);
    static long RETRY_DELAY = SECONDS.toMillis(5);
    static long MAX_RETRY_DELAY = MINUTES.toMillis(15);
    static long QUEUE_FETCH_DELAY = SECONDS.toMillis(1);
    private static final Supplier<LongSupplier> catchAllDelays = () -> exponential(RETRY_DELAY, MAX_RETRY_DELAY);

    private static final Logger LOG = LoggerFactory.getLogger(DistributionSubscriber.class);

    @Reference(name = "packageBuilder")
    private DistributionPackageBuilder packageBuilder;

    @Reference
    private SlingSettingsService slingSettings;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    @Reference(name = "precondition")
    private Precondition precondition;

    @Reference
    private DistributionMetricsService distributionMetricsService;

    @Reference
    BookKeeperFactory bookKeeperFactory;

    @Reference
    private SubscriberReadyStore subscriberReadyStore;

    private volatile Closeable idleReadyCheck; // NOSONAR

    private volatile IdleCheck idleCheck; // NOSONAR

    private Closeable packagePoller;

    private volatile CommandPoller commandPoller; // NOSONAR

    private BookKeeper bookKeeper;

    // Use a bounded internal buffer to allow reading further packages while working
    // on one at a time
    private final BlockingQueue<FullMessage<PackageMessage>> messageBuffer = new LinkedBlockingQueue<>(8);

    private Set<String> queueNames = Collections.emptySet();

    private Announcer announcer;

    private String subAgentName;

    private String pkgType;

    private volatile boolean running = true;
    private Thread queueThread;

    private LongSupplier catchAllDelay = catchAllDelays.get();

    private final Delay delay = new Delay();

    @Activate
    public void activate(SubscriberConfiguration config, BundleContext context, Map<String, Object> properties) {
        String subSlingId = requireNonNull(slingSettings.getSlingId());
        subAgentName = requireNotBlank(config.name());
        requireNonNull(config);
        requireNonNull(context);
        requireNonNull(packageBuilder);
        requireNonNull(slingSettings);
        requireNonNull(messagingProvider);
        requireNonNull(topics);
        requireNonNull(precondition);
        requireNonNull(bookKeeperFactory);

        Integer idleMillies = (Integer) properties.getOrDefault("idleMillies", SubscriberIdle.DEFAULT_IDLE_TIME_MILLIS);
        if (config.editable()) {
            commandPoller = new CommandPoller(messagingProvider, topics, subSlingId, subAgentName, delay::signal);
        }

        if (config.subscriberIdleCheck()) {
            AtomicBoolean readyHolder = subscriberReadyStore.getReadyHolder(subAgentName);

            idleCheck = new SubscriberIdle(idleMillies, SubscriberIdle.DEFAULT_FORCE_IDLE_MILLIS, readyHolder, () -> System.currentTimeMillis());
            idleReadyCheck = new SubscriberIdleCheck(context, idleCheck);
        } else {
            idleCheck = new NoopIdle();
        }

        queueNames = getNotEmpty(config.agentNames());
        pkgType = requireNonNull(packageBuilder.getType());

        Consumer<PackageStatusMessage> statusSender = messagingProvider.createSender(topics.getStatusTopic());
        Consumer<LogMessage> logSender = messagingProvider.createSender(topics.getDiscoveryTopic());

        String packageNodeName = escapeTopicName(messagingProvider.getServerUri(), topics.getPackageTopic());
        BookKeeperConfig bkConfig = new BookKeeperConfig(
                subAgentName,
                subSlingId,
                config.editable(),
                config.maxRetries(),
                config.packageHandling(),
                packageNodeName,
                config.contentPackageExtractorOverwritePrimaryTypesOfFolders());
        bookKeeper = bookKeeperFactory.create(packageBuilder, bkConfig, statusSender, logSender);

        long startOffset = bookKeeper.loadOffset() + 1;
        String assign = startOffset > 0 ? messagingProvider.assignTo(startOffset) : null;

        packagePoller = messagingProvider.createPoller(topics.getPackageTopic(), Reset.latest, assign,
                HandlerAdapter.create(PackageMessage.class, this::handlePackageMessage), HandlerAdapter.create(OffsetMessage.class, this::handleOffsetMessage));

        queueThread = startBackgroundThread(this::processQueue,
                format("Queue Processor for Subscriber agent %s", subAgentName));

        int announceDelay = Converters.standardConverter().convert(properties.get("announceDelay")).defaultValue(10000).to(Integer.class);
        announcer = new Announcer(subSlingId, subAgentName, queueNames,
                messagingProvider.createSender(topics.getDiscoveryTopic()), bookKeeper,
                config.maxRetries(), config.editable(), announceDelay);

        LOG.info("Started Subscriber agent {} at offset {}, subscribed to agent names {}", subAgentName, startOffset,
                queueNames);
    }

    public static String escapeTopicName(URI messagingUri, String topicName) {
        return String.format("%s%s_%s",
                messagingUri.getHost(),
                escape(messagingUri.getPath()),
                escape(topicName));
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
        try {
            queueThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Join interrupted");
        }
        LOG.info("Stopped Subscriber agent {}, subscribed to Publisher agent names {} with package builder {}",
                subAgentName, queueNames, pkgType);
    }

    public DistributionAgentState getState() {
        boolean isBlocked = bookKeeper.getPackageRetries().getSum() > 0;
        if (isBlocked) {
            return DistributionAgentState.BLOCKED;
        }
        return messageBuffer.size() > 0 ? DistributionAgentState.RUNNING : DistributionAgentState.IDLE;
    }

    private void handlePackageMessage(MessageInfo info, PackageMessage message) {
        if (shouldEnqueue(info, message)) {
            distributionMetricsService.getPackageJournalDistributionDuration()
                    .update((currentTimeMillis() - info.getCreateTime()), TimeUnit.MILLISECONDS);
            enqueue(new FullMessage<>(info, message));
        } else {
            try {
                bookKeeper.skipPackage(info.getOffset());
            } catch (PersistenceException | LoginException e) {
                LOG.warn("Error marking distribution package {} at offset={} as skipped", message, info.getOffset(), e);
            }
        }
    }

    private void handleOffsetMessage(MessageInfo info, OffsetMessage message) {
        bookKeeper.handleInitialOffset(info.getOffset());
    }

    private boolean shouldEnqueue(MessageInfo info, PackageMessage message) {
        if (!queueNames.contains(message.getPubAgentName())) {
            LOG.info("Skipping distribution package {} at offset={} (not subscribed)", message, info.getOffset());
            return false;
        }
        if (!pkgType.equals(message.getPkgType())) {
            LOG.warn("Skipping distribution package {} at offset={} (bad pkgType)", message, info.getOffset());
            return false;
        }
        return true;
    }

    /**
     * We block here if the buffer is full in order to limit the number of binary
     * packages fetched in memory. Note that each queued item contains the binary
     * package to be imported.
     */
    private void enqueue(FullMessage<PackageMessage> message) {
        try {
            while (running) {
                if (messageBuffer.offer(message, 1000, TimeUnit.MILLISECONDS)) {
                    distributionMetricsService.getItemsBufferSize().increment();
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        throw new RuntimeException();
    }

    private void processQueue() {
        LOG.info("Started Queue processor");
        while (running) {
            try {
                fetchAndProcessQueueItem();
            } catch (PreConditionTimeoutException e) {
                // Precondition timed out. We only log this on info level as it is no error
                LOG.info(e.getMessage());
                delay.await(RETRY_DELAY);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.debug(e.getMessage());
            } catch (Exception e) {
                // Catch all to prevent processing from stopping
                LOG.error("Error processing queue item", e);
                delay.await(catchAllDelay.getAsLong());
            }
        }
        LOG.info("Stopped Queue processor");
    }

    private void fetchAndProcessQueueItem() throws InterruptedException, IOException, LoginException,
            DistributionException, ImportPostProcessException {
        blockingSendStoredStatus();
        FullMessage<PackageMessage> item = blockingPeekQueueItem();
        try (Timer.Context context = distributionMetricsService.getProcessQueueItemDuration().time()) {
            processQueueItem(item);
            messageBuffer.remove();
            distributionMetricsService.getItemsBufferSize().decrement();
            catchAllDelay = catchAllDelays.get();
        }
    }

    /**
     * Send status stored in a previous run if exists
     */
    private void blockingSendStoredStatus() throws InterruptedException, IOException {
        try (Timer.Context context = distributionMetricsService.getSendStoredStatusDuration().time()) {
            int retry = 0;
            while (running) {
                if (bookKeeper.sendStoredStatus(retry)) {
                    return;
                }
                retry++;
            }
        }
        throw new InterruptedException("Shutting down");
    }

    private FullMessage<PackageMessage> blockingPeekQueueItem() throws InterruptedException {
        while (running) {
            FullMessage<PackageMessage> message = messageBuffer.peek();
            if (message != null) {
                return message;
            } else {
                delay.await(QUEUE_FETCH_DELAY);
            }
        }
        throw new InterruptedException("Shutting down");
    }

    private void processQueueItem(FullMessage<PackageMessage> item)
            throws PersistenceException, LoginException, DistributionException, ImportPostProcessException {
        MessageInfo info = item.getInfo();
        PackageMessage pkgMsg = item.getMessage();
        boolean skip = shouldSkip(info.getOffset());
        PackageMessage.ReqType type = pkgMsg.getReqType();
        try {
            idleCheck.busy(bookKeeper.getRetries(pkgMsg.getPubAgentName()), info.getCreateTime());
            if (skip) {
                bookKeeper.removePackage(pkgMsg, info.getOffset());
            } else if (type == INVALIDATE) {
                bookKeeper.invalidateCache(pkgMsg, info.getOffset());
            } else {
                bookKeeper.importPackage(pkgMsg, info.getOffset(), info.getCreateTime());
            }
        } finally {
            idleCheck.idle();
        }
    }

    private boolean shouldSkip(long offset) {
        return isCleared(offset) || isSkipped(offset);
    }

    private boolean isCleared(long offset) {
        return (commandPoller != null) && commandPoller.isCleared(offset);
    }

    private boolean isSkipped(long offset) {
        return waitPrecondition(offset) == Decision.SKIP;
    }

    private Decision waitPrecondition(long offset) {
        long endTime = System.currentTimeMillis() + PRECONDITION_TIMEOUT;
        while (System.currentTimeMillis() < endTime && running) {
            Decision decision = precondition.canProcess(subAgentName, offset);
            if (decision == Decision.WAIT) {
                delay.await(100);
            } else {
                return decision;
            }
        }
        throw new PreConditionTimeoutException(
                "Timeout waiting for distribution package at offset=" + offset + " on status topic");
    }

}
