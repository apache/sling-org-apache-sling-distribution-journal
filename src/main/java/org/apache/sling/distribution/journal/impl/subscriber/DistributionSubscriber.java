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
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeper;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperConfig;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperFactory;
import org.apache.sling.distribution.journal.impl.precondition.Precondition;
import org.apache.sling.distribution.journal.impl.precondition.Precondition.Decision;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
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
    private static final int PRECONDITION_TIMEOUT = 60;
    static int RETRY_DELAY = 5000;
    static int QUEUE_FETCH_DELAY = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(DistributionSubscriber.class);

    @Reference(name = "packageBuilder")
    private DistributionPackageBuilder packageBuilder;

    @Reference
    private SlingSettingsService slingSettings;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    @Reference
    private JournalAvailable journalAvailable;

    @Reference(name = "precondition")
    private Precondition precondition;

    @Reference
    private DistributionMetricsService distributionMetricsService;

    @Reference
    BookKeeperFactory bookKeeperFactory;

    @Reference
    private SubscriberReadyStore subscriberReadyStore;
    
    private Optional<SubscriberIdle> subscriberIdle;
    
    private Closeable packagePoller;

    private Optional<CommandPoller> commandPoller;

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

    @Activate
    public void activate(SubscriberConfiguration config, BundleContext context, Map<String, Object> properties) {
        String subSlingId = requireNonNull(slingSettings.getSlingId());
        subAgentName = requireNonNull(config.name());
        requireNonNull(config);
        requireNonNull(context);
        requireNonNull(packageBuilder);
        requireNonNull(slingSettings);
        requireNonNull(messagingProvider);
        requireNonNull(topics);
        requireNonNull(precondition);
        requireNonNull(bookKeeperFactory);

        if (config.subscriberIdleCheck()) {
            // Unofficial config (currently just for test)
            Integer idleMillies = (Integer) properties.getOrDefault("idleMillies", SubscriberIdle.DEFAULT_IDLE_TIME_MILLIS);
            AtomicBoolean readyHolder = subscriberReadyStore.getReadyHolder(subAgentName);
            subscriberIdle = Optional.of(new SubscriberIdle(context, idleMillies, readyHolder));
        } else {
            subscriberIdle = Optional.empty();
        }
        
        queueNames = getNotEmpty(config.agentNames());
        pkgType = requireNonNull(packageBuilder.getType());

        Consumer<PackageStatusMessage> sender = messagingProvider.createSender(topics.getStatusTopic());
        BookKeeperConfig bkConfig = new BookKeeperConfig(subAgentName, subSlingId, config.editable(), config.maxRetries(), config.packageHandling());
        bookKeeper = bookKeeperFactory.create(packageBuilder, bkConfig, sender);
        
        long startOffset = bookKeeper.loadOffset() + 1;
        String assign = messagingProvider.assignTo(startOffset);

        packagePoller = messagingProvider.createPoller(topics.getPackageTopic(), Reset.earliest, assign,
                HandlerAdapter.create(PackageMessage.class, this::handlePackageMessage));

        if (config.editable()) {
            commandPoller = Optional.of(new CommandPoller(messagingProvider, topics, subSlingId, subAgentName));
        } else {
            commandPoller = Optional.empty();
        }

        queueThread = startBackgroundThread(this::processQueue,
                format("Queue Processor for Subscriber agent %s", subAgentName));

        int announceDelay = PropertiesUtil.toInteger(properties.get("announceDelay"), 10000);
        announcer = new Announcer(subSlingId, subAgentName, queueNames, messagingProvider.createSender(topics.getDiscoveryTopic()), bookKeeper,
                config.maxRetries(), config.editable(), announceDelay);

        LOG.info("Started Subscriber agent {} at offset {}, subscribed to agent names {}", subAgentName, startOffset, queueNames);
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
         * See SLING-9340, OAK-2609 and https://jackrabbit.apache.org/oak/docs/dos_and_donts.html
         */

        IOUtils.closeQuietly(announcer, bookKeeper, 
                packagePoller);
        commandPoller.ifPresent(IOUtils::closeQuietly);
        subscriberIdle.ifPresent(IOUtils::closeQuietly);
        running = false;
        try {
            queueThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Join interrupted");
        }
        String msg = String.format(
                "Stopped Subscriber agent %s, subscribed to Publisher agent names %s with package builder %s",
                subAgentName, queueNames, pkgType);
        LOG.info(msg);
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
            enqueue(new FullMessage<PackageMessage>(info, message));
        } else {
            try {
                bookKeeper.skipPackage(info.getOffset());
            } catch (PersistenceException | LoginException e) {
                LOG.info("Error marking message at offset {} as skipped", info.getOffset(), e);
            }
        }
    }

    private boolean shouldEnqueue(MessageInfo info, PackageMessage message) {
        if (!queueNames.contains(message.getPubAgentName())) {
            LOG.info("Skipping package for Publisher agent {} at offset {} (not subscribed)", message.getPubAgentName(), info.getOffset());
            return false;
        }
        if (!pkgType.equals(message.getPkgType())) {
            LOG.warn("Skipping package with type {} at offset {}", message.getPkgType(), info.getOffset());
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
            fetchAndProcessQueueItem();
        }
        LOG.info("Stopped Queue processor");
    }

    private void fetchAndProcessQueueItem() {
        try {
            
            if (! blockingSendStoredStatus()) {
                return;
            }

            Optional<FullMessage<PackageMessage>> item = blockingPeekQueueItem();
            if (!item.isPresent()) {
                return;
            }

            try (Timer.Context context = distributionMetricsService.getProcessQueueItemDuration().time()) {
                processQueueItem(item.get());
                messageBuffer.remove();
                distributionMetricsService.getItemsBufferSize().decrement();
            }

        } catch (PreConditionTimeoutException e) {
            // Precondition timed out. We only log this on info level as it is no error
            LOG.info(e.getMessage());
            delay(RETRY_DELAY);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            // Catch all to prevent processing from stopping
            LOG.error("Error processing queue item", e);
            delay(RETRY_DELAY);
        }
    }

    /**
     * Send status stored in a previous run if exists
     *
     * @return {@code true} if the status has been sent ;
     *         {@code false} otherwise.
     */
    private boolean blockingSendStoredStatus() {
        try (Timer.Context context = distributionMetricsService.getSendStoredStatusDuration().time()) {
            for (int retry = 0 ; running && ! bookKeeper.sendStoredStatus(retry) ; retry++);
        } catch (IOException e) {
            LOG.warn("Error in timer close", e);
        }
        return running;
    }

    private Optional<FullMessage<PackageMessage>> blockingPeekQueueItem() throws InterruptedException {
        while (running) {
            FullMessage<PackageMessage> message = messageBuffer.peek();
            if (message != null) {
                return Optional.of(message);
            } else {
                Thread.sleep(QUEUE_FETCH_DELAY);
            }
        }
        return Optional.empty();
    }

    private void processQueueItem(FullMessage<PackageMessage> item) throws PersistenceException, LoginException, DistributionException {
        MessageInfo info = item.getInfo();
        PackageMessage pkgMsg = item.getMessage();
        boolean skip = shouldSkip(info.getOffset());
        try {
            subscriberIdle.ifPresent(SubscriberIdle::busy);
            if (skip) {
                bookKeeper.removePackage(pkgMsg, info.getOffset());
            } else {
                bookKeeper.importPackage(pkgMsg, info.getOffset(), info.getCreateTime());
            }
        } finally {
            subscriberIdle.ifPresent(SubscriberIdle::idle);
        }
    }

    private boolean shouldSkip(long offset) {
        boolean cleared = commandPoller.isPresent() && commandPoller.get().isCleared(offset);
        Decision decision = waitPrecondition(offset);
        return cleared || decision == Decision.SKIP;
    }

    private Decision waitPrecondition(long offset) {
        Decision decision = Precondition.Decision.WAIT;
        long endTime = System.currentTimeMillis() + PRECONDITION_TIMEOUT * 1000;
        while (decision == Decision.WAIT && System.currentTimeMillis() < endTime && running) {
            decision = precondition.canProcess(subAgentName, offset);
            if (decision == Decision.WAIT) {
                delay(100);
            } else {
                return decision;
            }
        }
        throw new PreConditionTimeoutException("Timeout waiting for package offset " + offset + " on status topic.");
    }

    private static void delay(long delayInMs) {
        try {
            Thread.sleep(delayInMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
