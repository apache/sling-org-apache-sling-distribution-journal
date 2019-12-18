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
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;
import static org.apache.sling.distribution.journal.HandlerAdapter.create;
import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.PACKAGE_MSG;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_OFFSET;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_TIMESTAMP;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.vault.packaging.Packaging;
import org.apache.sling.api.resource.LoginException;
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
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.queue.QueueItemFactory;
import org.apache.sling.distribution.journal.impl.queue.impl.SubQueue;
import org.apache.sling.distribution.journal.impl.shared.AgentState;
import org.apache.sling.distribution.journal.impl.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.impl.shared.SimpleDistributionResponse;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
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
import org.slf4j.MDC;

import com.google.protobuf.GeneratedMessage;

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

    private volatile Thread queueProcessor;

    private PackageHandler packageHandler;

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

        queueNames = getNotEmpty(config.agentNames());
        int maxRetries = config.maxRetries();
        boolean editable = config.editable();

        bookKeeper = new BookKeeper(resolverFactory, distributionMetricsService, eventAdmin,
                sender(topics.getStatusTopic()), subAgentName, subSlingId, editable, maxRetries);
        long startOffset = bookKeeper.loadOffset() + 1;
        String assign = messagingProvider.assignTo(startOffset);

        packagePoller = messagingProvider.createPoller(topics.getPackageTopic(), Reset.earliest, assign,
                create(PackageMessage.class, this::handlePackageMessage));

        commandPoller = new CommandPoller(messagingProvider, topics, subSlingId, subAgentName, editable);

        queueProcessor = startBackgroundThread(this::processQueue,
                format("Queue Processor for Subscriber agent %s", subAgentName));

        int announceDelay = PropertiesUtil.toInteger(properties.get("announceDelay"), 10000);
        announcer = new Announcer(subSlingId, subAgentName, queueNames, sender(topics.getDiscoveryTopic()), bookKeeper,
                maxRetries, config.editable(), announceDelay);

        pkgType = requireNonNull(packageBuilder.getType());
        boolean errorQueueEnabled = (maxRetries >= 0);
        String msg = format(
                "Started Subscriber agent %s at offset %s, subscribed to agent names %s with package builder %s editable %s maxRetries %s errorQueueEnabled %s",
                subAgentName, startOffset, queueNames, pkgType, config.editable(), maxRetries, errorQueueEnabled);
        LOG.info(msg);
        Dictionary<String, Object> props = createServiceProps(config);
        componentReg = context.registerService(DistributionAgent.class, this, props);
        ContentPackageExtractor extractor = new ContentPackageExtractor(packaging, config.packageHandling());
        packageHandler = new PackageHandler(packageBuilder, extractor);
    }

    private <T extends GeneratedMessage> Consumer<T> sender(String topic) {
        MessageSender<T> sender = messagingProvider.createSender();
        return msg -> sender.send(topic, msg);
    }

    private Set<String> getNotEmpty(String[] agentNames) {
        return asList(agentNames).stream().filter(StringUtils::isNotBlank).collect(toSet());
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
        IOUtils.closeQuietly(announcer);
        IOUtils.closeQuietly(bookKeeper);
        componentReg.unregister();
        IOUtils.closeQuietly(packagePoller);
        IOUtils.closeQuietly(commandPoller);
        running = false;
        Thread interrupter = this.queueProcessor;
        if (interrupter != null) {
            interrupter.interrupt();
        }
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
        DistributionQueueItem head = queueItemsBuffer.stream().filter(item -> isIn(queueName, item)).findFirst()
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
        if (!queueNames.contains(message.getPubAgentName())) {
            LOG.info(String.format("Skipping package for Publisher agent %s (not subscribed)", message.getPubAgentName()));
            return;
        }
        if (!pkgType.equals(message.getPkgType())) {
            LOG.warn(String.format("Skipping package with type %s", message.getPkgType()));
            return;
        }
        DistributionQueueItem queueItem = QueueItemFactory.fromPackage(info, message, true);
        enqueue(queueItem);
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
            throw new InterruptedException();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException();
        }
    }

    private void processQueue() {
        LOG.info("Started Queue processor");
        while (!Thread.interrupted()) {
            try {
                fetchAndProcessQueueItem();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("Stopped Queue processor");
    }

    private void fetchAndProcessQueueItem() throws InterruptedException {
        try {
            // send status stored in a previous run if exists
            bookKeeper.sendStoredStatus();
            // block until an item is available
            DistributionQueueItem item = blockingPeekQueueItem();
            // and then process it
            try (Timer.Context context = distributionMetricsService.getProcessQueueItemDuration().time()) {
                processQueueItem(item);
                queueItemsBuffer.remove();
                distributionMetricsService.getItemsBufferSize().decrement();
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable t) {
            // Catch all to prevent processing from stopping
            LOG.error("Error processing queue item", t);
            Thread.sleep(RETRY_DELAY);
        }
    }

    private DistributionQueueItem blockingPeekQueueItem() throws InterruptedException {
        while (true) {
            DistributionQueueItem queueItem = queueItemsBuffer.peek();
            if (queueItem != null) {
                return queueItem;
            } else {
                Thread.sleep(QUEUE_FETCH_DELAY);
            }
        }
    }

    private void processQueueItem(DistributionQueueItem queueItem) throws Exception {
        long offset = queueItem.get(RECORD_OFFSET, Long.class);
        boolean skip;
        try {
            skip = commandPoller.isCleared(offset) || cannotProcess(offset);
        } catch (IllegalStateException e) {
            /**
             * This will occur when the precondition times out.
             */
            LOG.info(e.getMessage());
            Thread.sleep(RETRY_DELAY);
            return;
        }
        PackageMessage pkgMsg = queueItem.get(PACKAGE_MSG, PackageMessage.class);
        if (skip) {
            bookKeeper.removePackage(pkgMsg, offset);
        } else {
            long createdTime = queueItem.get(RECORD_TIMESTAMP, Long.class);
            importPackage(pkgMsg, offset, createdTime);
        }
    }

    private boolean cannotProcess(long offset) {
        return !precondition.canProcess(offset, PRECONDITION_TIMEOUT);
    }

    /**
     * We aim at processing the packages exactly once. Processing the packages
     * exactly once is possible with the following conditions
     *
     * I. The package importer is configured to disable auto-committing changes.
     *
     * II. A single commit aggregates three content updates
     *
     * C1. install the package C2. store the processing status C3. store the offset
     * processed
     *
     * Some package importers require auto-saving or issue partial commits before
     * failing. For those packages importers, we aim at processing packages at least
     * once, thanks to the order in which the content updates are applied.
     */
    private void importPackage(PackageMessage pkgMsg, long offset, long createdTime) throws DistributionException {
        LOG.info(format("Importing distribution package %s of type %s at offset %s", pkgMsg.getPkgId(),
                pkgMsg.getReqType(), offset));
        bookKeeper.addPackageMDC(pkgMsg);
        try (Timer.Context context = distributionMetricsService.getImportedPackageDuration().time();
                ResourceResolver importerResolver = getServiceResolver("importer")) {
            packageHandler.apply(importerResolver, pkgMsg);
            bookKeeper.imported(importerResolver, pkgMsg, offset, createdTime);
            importerResolver.commit();
        } catch (LoginException | IOException | RuntimeException e) {
            bookKeeper.failure(pkgMsg, offset, e);
        } finally {
            MDC.clear();
        }
    }

    private ResourceResolver getServiceResolver(String subService) throws LoginException {
        return resolverFactory.getServiceResourceResolver(singletonMap(SUBSERVICE, subService));
    }

}
