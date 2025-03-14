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

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toSet;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import javax.annotation.ParametersAreNonnullByDefault;

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
import org.apache.sling.distribution.journal.bookkeeper.BookKeeper;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperConfig;
import org.apache.sling.distribution.journal.bookkeeper.BookKeeperFactory;
import org.apache.sling.distribution.journal.bookkeeper.SubscriberMetrics;
import org.apache.sling.distribution.journal.impl.precondition.Precondition;
import org.apache.sling.distribution.journal.impl.precondition.Precondition.Decision;
import org.apache.sling.distribution.journal.messages.LogMessage;
import org.apache.sling.distribution.journal.messages.OffsetMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.shared.Delay;
import org.apache.sling.distribution.journal.shared.OnlyOnLeader;
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

    private static final long PRECONDITION_TIMEOUT_MILLIS = SECONDS.toMillis(60);
    static long RETRY_DELAY_MILLIS = SECONDS.toMillis(5);
    static long MAX_RETRY_DELAY_MILLIS = MINUTES.toMillis(15);
    static long QUEUE_FETCH_DELAY_MILLIS = SECONDS.toMillis(1);
    private static final Supplier<LongSupplier> catchAllDelays = () -> exponential(RETRY_DELAY_MILLIS, MAX_RETRY_DELAY_MILLIS);

    private static final Logger LOG = LoggerFactory.getLogger(DistributionSubscriber.class);

    @Reference(name = "packageBuilder")
    private DistributionPackageBuilder packageBuilder;

    @Reference
    private SlingSettingsService slingSettings;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference(name = "precondition")
    private Precondition precondition;

    @Reference
    private MetricsService metricsService;

    @Reference
    BookKeeperFactory bookKeeperFactory;

    @Reference
    private SubscriberReadyStore subscriberReadyStore;

    @Reference
    private OnlyOnLeader onlyOnLeader;

    private SubscriberMetrics subscriberMetrics;

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
	private AtomicReference<DistributionAgentState> state = new AtomicReference<DistributionAgentState>(DistributionAgentState.IDLE);

    @Activate
    public void activate(SubscriberConfiguration config, BundleContext context, Map<String, Object> properties) {
        String subSlingId = requireNonNull(slingSettings.getSlingId());
        subAgentName = requireNotBlank(config.name());
        requireNonNull(config);
        requireNonNull(context);
        requireNonNull(metricsService);
        requireNonNull(packageBuilder);
        requireNonNull(slingSettings);
        requireNonNull(messagingProvider);
        requireNonNull(precondition);
        requireNonNull(bookKeeperFactory);
        this.subscriberMetrics = new SubscriberMetrics(metricsService, subAgentName, getFirst(config.agentNames()), config.editable());

        if (config.subscriberIdleCheck()) {
            AtomicBoolean readyHolder = subscriberReadyStore.getReadyHolder(subAgentName);
            idleCheck = new SubscriberReady(subAgentName, config.idleMillies(), config.forceReadyMillies(), config.acceptableAgeDiffMs(), readyHolder, System::currentTimeMillis);
            idleReadyCheck = new SubscriberIdleCheck(context, idleCheck, config.subscriberIdleTags());
        } else {
            idleCheck = new NoopIdle();
        }

        queueNames = getNotEmpty(config.agentNames());
        pkgType = requireNonNull(packageBuilder.getType());

        Consumer<PackageStatusMessage> statusSender = messagingProvider.createSender(Topics.STATUS_TOPIC);
        Consumer<LogMessage> logSender = messagingProvider.createSender(Topics.DISCOVERY_TOPIC);

        String packageNodeName = escapeTopicName(messagingProvider.getServerUri(), Topics.PACKAGE_TOPIC);
        BookKeeperConfig bkConfig = new BookKeeperConfig(
                subAgentName,
                subSlingId,
                config.editable(),
                config.maxRetries(),
                config.packageHandling(),
                packageNodeName,
                config.contentPackageExtractorOverwritePrimaryTypesOfFolders());
        bookKeeper = bookKeeperFactory.create(packageBuilder, bkConfig, statusSender, logSender, this.subscriberMetrics);
        
        if (config.editable()) {
        	Consumer<Long> clearHandler = (Long offset) -> {
        		bookKeeper.storeClearOffset(offset);
        		delay.signal();
        	};
            commandPoller = new CommandPoller(messagingProvider, subSlingId, subAgentName, bookKeeper.getClearOffset(), clearHandler);
        }

        long startOffset = bookKeeper.loadOffset() + 1;
        String assign = startOffset > 0 ? messagingProvider.assignTo(startOffset) : null;

        packagePoller = messagingProvider.createPoller(Topics.PACKAGE_TOPIC, Reset.latest, assign,
                HandlerAdapter.create(PackageMessage.class, this::handlePackageMessage), HandlerAdapter.create(OffsetMessage.class, this::handleOffsetMessage));

        int announceDelay = Converters.standardConverter().convert(properties.get("announceDelay")).defaultValue(10000).to(Integer.class);
        announcer = new Announcer(subSlingId, subAgentName, queueNames,
                messagingProvider.createSender(Topics.DISCOVERY_TOPIC), bookKeeper,
                config.maxRetries(), config.editable(), announceDelay);

        LOG.info("Started Subscriber agent={} at offset={}, subscribed to agent names {}, readyCheck={}", subAgentName, startOffset,
                queueNames, config.subscriberIdleCheck());
    }

    private String getFirst(String[] agentNames) {
        return agentNames != null && agentNames.length > 0 ? agentNames[0] : "";
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
        LOG.info("Stopped Subscriber agent {}, subscribed to Publisher agent names {} with package builder {}",
                subAgentName, queueNames, pkgType);
    }

    public DistributionAgentState getState() {
        boolean isBlocked = bookKeeper.getPackageRetries().getSum() > 0;
        return (isBlocked) ? DistributionAgentState.BLOCKED : state.get();
    }

    private void handlePackageMessage(MessageInfo info, PackageMessage message) {
    	boolean done = false;
    	while (!done && running) {
    		done = tryProcess(info, message);
        }
    }

	public boolean tryProcess(MessageInfo info, PackageMessage message) {
		if (shouldSkip(info, message)) {
            try {
                bookKeeper.skipPackage(info.getOffset());
            } catch (PersistenceException | LoginException e) {
                LOG.warn("Error marking distribution package {} at offset={} as skipped", message, info.getOffset(), e);
            }
            return true;
        }
        subscriberMetrics.getPackageJournalDistributionDuration()
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
        try (Timer.Context context = subscriberMetrics.getSendStoredStatusDuration().time()) {
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
            throws PersistenceException, LoginException, DistributionException, ImportPostProcessException, InterruptedException {
        blockingSendStoredStatus();
        boolean skip = shouldRemove(info.getOffset());
        PackageMessage.ReqType type = pkgMsg.getReqType();
        try {
        	this.state.set(DistributionAgentState.RUNNING);
            idleCheck.busy(bookKeeper.getRetries(pkgMsg.getPubAgentName()), info.getCreateTime());
            long importStartTime = System.currentTimeMillis();
            if (skip) {
                bookKeeper.removePackage(pkgMsg, info.getOffset());
            } else if (type == INVALIDATE) {
                bookKeeper.invalidateCache(pkgMsg, info.getOffset(), importStartTime);
            } else {
                bookKeeper.importPackage(pkgMsg, info.getOffset(), info.getCreateTime(), importStartTime);
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
        final String msg = String.format("Timeout after %s seconds while waiting to meet the preconditions"
                + " to import the distribution package at offset %s on topic status",
                MILLISECONDS.toSeconds(PRECONDITION_TIMEOUT_MILLIS),
                offset);
        throw new PreConditionTimeoutException(msg);
    }

}
