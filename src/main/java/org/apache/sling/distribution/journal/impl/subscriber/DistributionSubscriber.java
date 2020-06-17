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
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.PACKAGE_MSG;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_OFFSET;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_TIMESTAMP;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.vault.packaging.Packaging;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestState;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.precondition.Precondition;
import org.apache.sling.distribution.journal.impl.queue.QueueItemFactory;
import org.apache.sling.distribution.journal.impl.queue.impl.SubQueue;
import org.apache.sling.distribution.journal.impl.shared.AgentState;
import org.apache.sling.distribution.journal.impl.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.impl.shared.SimpleDistributionResponse;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.log.spi.DistributionLog;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.apache.sling.settings.SlingSettingsService;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventAdmin;
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
public class DistributionSubscriber implements DistributionAgent {
    private static final int PRECONDITION_TIMEOUT = 60;
    static int RETRY_DELAY = 5000;
    static int QUEUE_FETCH_DELAY = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(DistributionSubscriber.class);

    private static final Set<DistributionRequestType> SUPPORTED_REQ_TYPES = Collections.emptySet();

    private static final DistributionQueueItem STOPPED_ITEM = new DistributionQueueItem("stop-item", emptyMap());

    @Reference(name = "packageBuilder")
    private DistributionPackageBuilder packageBuilder;

    @Reference
    private SlingSettingsService slingSettings;

    @Reference
    private ResourceResolverFactory resolverFactory;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    @Reference
    private EventAdmin eventAdmin;

    @Reference
    private JournalAvailable journalAvailable;

    @Reference(name = "precondition")
    private Precondition precondition;

    @Reference
    private DistributionMetricsService distributionMetricsService;

    @Reference
    private Packaging packaging;

    @Reference
    private SubscriberReadyStore subscriberReadyStore;
    
    private Optional<SubscriberIdle> subscriberIdle;
    
    private ServiceRegistration<DistributionAgent> componentReg;

    private Closeable packagePoller;

    private CommandPoller commandPoller;

    private BookKeeper bookKeeper;

    // Use a bounded internal buffer to allow reading further packages while working
    // on one at a time
    private final BlockingQueue<DistributionQueueItem> queueItemsBuffer = new LinkedBlockingQueue<>(8);

    private Set<String> queueNames = Collections.emptySet();

    private Announcer announcer;

    private String subAgentName;

    private String pkgType;

    private volatile boolean running = true;

    @Activate
    public void activate(SubscriberConfiguration config, BundleContext context, Map<String, Object> properties) {
        String subSlingId = requireNonNull(slingSettings.getSlingId());
        subAgentName = requireNonNull(config.name());
        requireNonNull(config);
        requireNonNull(context);
        requireNonNull(packageBuilder);
        requireNonNull(slingSettings);
        requireNonNull(resolverFactory);
        requireNonNull(messagingProvider);
        requireNonNull(topics);
        requireNonNull(eventAdmin);
        requireNonNull(precondition);

        if (config.subscriberIdleCheck()) {
            // Unofficial config (currently just for test)
            Integer idleMillies = (Integer) properties.getOrDefault("idleMillies", SubscriberIdle.DEFAULT_IDLE_TIME_MILLIS);
            AtomicBoolean readyHolder = subscriberReadyStore.getReadyHolder(subAgentName);
            subscriberIdle = Optional.of(new SubscriberIdle(context, idleMillies, readyHolder));
        } else {
            subscriberIdle = Optional.empty();
        }
        
        queueNames = getNotEmpty(config.agentNames());
        int maxRetries = config.maxRetries();
        boolean editable = config.editable();

        ContentPackageExtractor extractor = new ContentPackageExtractor(packaging, config.packageHandling());
        PackageHandler packageHandler = new PackageHandler(packageBuilder, extractor);
        Consumer<PackageStatusMessage> sender = messagingProvider.createSender(topics.getStatusTopic());
        bookKeeper = new BookKeeper(resolverFactory, distributionMetricsService, packageHandler, eventAdmin,
                sender, subAgentName, subSlingId, editable, maxRetries);
        
        long startOffset = bookKeeper.loadOffset() + 1;
        String assign = messagingProvider.assignTo(startOffset);

        packagePoller = messagingProvider.createPoller(topics.getPackageTopic(), Reset.earliest, assign,
                HandlerAdapter.create(PackageMessage.class, this::handlePackageMessage));

        commandPoller = new CommandPoller(messagingProvider, topics, subSlingId, subAgentName, editable);

        startBackgroundThread(this::processQueue,
                format("Queue Processor for Subscriber agent %s", subAgentName));

        int announceDelay = PropertiesUtil.toInteger(properties.get("announceDelay"), 10000);
        announcer = new Announcer(subSlingId, subAgentName, queueNames, messagingProvider.createSender(topics.getDiscoveryTopic()), bookKeeper,
                maxRetries, config.editable(), announceDelay);

        pkgType = requireNonNull(packageBuilder.getType());
        boolean errorQueueEnabled = (maxRetries >= 0);
        String msg = format(
                "Started Subscriber agent %s at offset %s, subscribed to agent names %s with package builder %s editable %s maxRetries %s errorQueueEnabled %s",
                subAgentName, startOffset, queueNames, pkgType, config.editable(), maxRetries, errorQueueEnabled);
        LOG.info(msg);
        Dictionary<String, Object> props = createServiceProps(config);
        componentReg = context.registerService(DistributionAgent.class, this, props);
    }

    private Set<String> getNotEmpty(String[] agentNames) {
        return Arrays.stream(agentNames).filter(StringUtils::isNotBlank).collect(toSet());
    }

    private Dictionary<String, Object> createServiceProps(SubscriberConfiguration config) {
        Dictionary<String, Object> props = new Hashtable<>();
        props.put("name", config.name());
        props.put("title", config.name());
        props.put("details", config.name());
        props.put("agentNames", config.agentNames());
        props.put("editable", config.editable());
        props.put("maxRetries", config.maxRetries());
        props.put("packageBuilder.target", config.packageBuilder_target());
        props.put("precondition.target", config.precondition_target());
        props.put("webconsole.configurationFactory.nameHint", config.webconsole_configurationFactory_nameHint());
        return props;
    }

    @Deactivate
    public void deactivate() {

        /*
         * Note that we don't interrupt blocking calls using Thread.interrupt()
         * because interrupts can stop the Apache Oak repository.
         *
         * See SLING-9340, OAK-2609 and https://jackrabbit.apache.org/oak/docs/dos_and_donts.html
         */

        componentReg.unregister();
        IOUtils.closeQuietly(announcer, bookKeeper, 
                packagePoller, commandPoller);
        subscriberIdle.ifPresent(IOUtils::closeQuietly);
        running = false;
        String msg = String.format(
                "Stopped Subscriber agent %s, subscribed to Publisher agent names %s with package builder %s",
                subAgentName, queueNames, pkgType);
        LOG.info(msg);
    }

    @Nonnull
    @Override
    public Iterable<String> getQueueNames() {
        return queueNames;
    }

    @Override
    public DistributionQueue getQueue(@Nonnull String queueName) {
        DistributionQueueItem head = queueItemsBuffer.stream()
                .filter(item -> isIn(queueName, item))
                .findFirst()
                .orElse(null);
        return new SubQueue(queueName, head, bookKeeper.getPackageRetries());
    }

    private boolean isIn(String queueName, DistributionQueueItem queueItem) {
        PackageMessage packageMsg = queueItem.get(QueueItemFactory.PACKAGE_MSG, PackageMessage.class);
        return queueName.equals(packageMsg.getPubAgentName());
    }

    @Nonnull
    @Override
    public DistributionLog getLog() {
        return this::emptyDistributionLog;
    }

    private List<String> emptyDistributionLog() {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public DistributionAgentState getState() {
        return AgentState.getState(this);
    }

    @Nonnull
    @Override
    public DistributionResponse execute(ResourceResolver resourceResolver, DistributionRequest request) {
        return executeUnsupported(request);
    }

    @Nonnull
    private DistributionResponse executeUnsupported(DistributionRequest request) {
        String msg = format("Request type %s is not supported by this agent, expected one of %s",
                request.getRequestType(), SUPPORTED_REQ_TYPES);
        LOG.info(msg);
        return new SimpleDistributionResponse(DistributionRequestState.DROPPED, msg);
    }

    private void handlePackageMessage(MessageInfo info, PackageMessage message) {
        if (shouldEnqueue(info, message)) {
            DistributionQueueItem queueItem = QueueItemFactory.fromPackage(info, message, true);
            enqueue(queueItem);
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
    private void enqueue(DistributionQueueItem queueItem) {
        try {
            while (running) {
                if (queueItemsBuffer.offer(queueItem, 1000, TimeUnit.MILLISECONDS)) {
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

            DistributionQueueItem item = blockingPeekQueueItem();
            if (STOPPED_ITEM == item) {
                return;
            }

            try (Timer.Context context = distributionMetricsService.getProcessQueueItemDuration().time()) {
                processQueueItem(item);
            } finally {
                subscriberIdle.ifPresent(SubscriberIdle::idle);
            }

        } catch (TimeoutException e) {
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

    private DistributionQueueItem blockingPeekQueueItem() throws InterruptedException {
        while (running) {
            DistributionQueueItem queueItem = queueItemsBuffer.peek();
            if (queueItem != null) {
                return queueItem;
            } else {
                Thread.sleep(QUEUE_FETCH_DELAY);
            }
        }
        return STOPPED_ITEM;
    }

    private void processQueueItem(DistributionQueueItem queueItem) throws PersistenceException, LoginException, DistributionException, TimeoutException {
        long offset = queueItem.get(RECORD_OFFSET, Long.class);
        PackageMessage pkgMsg = queueItem.get(PACKAGE_MSG, PackageMessage.class);
        boolean skip = shouldSkip(offset);
        subscriberIdle.ifPresent(SubscriberIdle::busy);
        if (skip) {
            bookKeeper.removePackage(pkgMsg, offset);
        } else {
            long createdTime = queueItem.get(RECORD_TIMESTAMP, Long.class);
            bookKeeper.importPackage(pkgMsg, offset, createdTime);
        }
        queueItemsBuffer.remove();
        distributionMetricsService.getItemsBufferSize().decrement();
    }

    private boolean shouldSkip(long offset) throws TimeoutException {
        return commandPoller.isCleared(offset) || !precondition.canProcess(subAgentName, offset, PRECONDITION_TIMEOUT);
    }

    private static void delay(long delayInMs) {
        try {
            Thread.sleep(delayInMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
