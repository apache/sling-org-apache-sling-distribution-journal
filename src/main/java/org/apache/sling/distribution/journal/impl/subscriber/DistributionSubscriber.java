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

import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.PACKAGE_MSG;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_OFFSET;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_TIMESTAMP;
import static org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage.Status.IMPORTED;
import static org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage.Status.REMOVED;
import static org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage.Status.REMOVED_FAILED;
import static org.apache.sling.distribution.journal.HandlerAdapter.create;
import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;

import org.apache.sling.commons.osgi.PropertiesUtil;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.impl.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.impl.shared.SimpleDistributionResponse;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.impl.shared.AgentState;
import org.apache.sling.distribution.journal.messages.Messages.CommandMessage;
import org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage.Status;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.commons.jackrabbit.SimpleReferenceBinary;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestState;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.log.spi.DistributionLog;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.apache.sling.serviceusermapping.ServiceUserMapped;
import org.apache.sling.settings.SlingSettingsService;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.sling.distribution.journal.impl.queue.QueueItemFactory;
import org.apache.sling.distribution.journal.impl.queue.impl.PackageRetries;
import org.apache.sling.distribution.journal.impl.queue.impl.SubQueue;
import org.apache.sling.distribution.journal.messages.Messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.Reset;

/**
 * A Subscriber SCD agent which consumes messages produced by a {@code DistributionPublisher} agent.
 */
@Component(
        service = {},
        immediate = true,
        property = {"announceDelay=10000"},
        configurationPid = "org.apache.sling.distribution.journal.impl.subscriber.DistributionSubscriberFactory")
@Designate(ocd = SubscriberConfiguration.class, factory = true)
@ParametersAreNonnullByDefault
public class DistributionSubscriber implements DistributionAgent {
    private static final int PRECONDITION_TIMEOUT = 60;
    private static final int RETRY_SEND_DELAY = 1000;
    static int RETRY_DELAY = 5000;
    static int QUEUE_FETCH_DELAY = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(DistributionSubscriber.class);
    
    private static final Set<DistributionRequestType> SUPPORTED_REQ_TYPES = Collections.emptySet();

    private static final DistributionQueueItem STOPPED_ITEM = new DistributionQueueItem("stop-item", Collections.emptyMap());

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
    JournalAvailable journalAvailable;

    @Reference(name = "precondition")
    Precondition precondition;

    @Reference
    private DistributionMetricsService distributionMetricsService;
    
    @Reference
    private ServiceUserMapped mappedUser;

    private ServiceRegistration<DistributionAgent> componentReg;

    private final PackageRetries packageRetries = new PackageRetries();

    private Closeable packagePoller;

    private Closeable commandPoller;

    private LocalStore processedOffsets;

    private LocalStore processedStatuses;


    private final AtomicLong clearOffset = new AtomicLong(-1);

    // Use a bounded internal buffer to allow reading further packages while working on one at a time
    private final BlockingQueue<DistributionQueueItem> queueItemsBuffer = new LinkedBlockingQueue<>(8);

    private Set<String> queueNames = Collections.emptySet();

    private MessageSender<PackageStatusMessage> sender;

    private Announcer announcer;

    private String subAgentName;

    private String subSlingId;

    private String pkgType;

    private int maxRetries;

    private boolean errorQueueEnabled;

    private boolean editable;

    volatile boolean running = true;
    
    @Activate
    public void activate(SubscriberConfiguration config, BundleContext context, Map<String, Object> properties) {

        subSlingId = requireNonNull(slingSettings.getSlingId());
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

        maxRetries = config.maxRetries();
        // Error queues are enabled when the number
        // of retry attempts is limited ; disabled otherwise
        errorQueueEnabled = (maxRetries >= 0);

        editable = config.editable();

        // The offset store is identified by the agentName only.
        //
        // With non clustered publish instances deployment, each
        // instance stores the offset in its own node store, thus
        // avoiding mix ups. Moreover, when cloning an instance
        // from a node store, the cloned instance will implicitly
        // recover the offsets and start from the last processed
        // offset.
        //
        // With clustered publish instances deployment, only one
        // Subscriber agent must run on the cluster in order to
        // avoid mix ups.
        //
        // The clustered and non clustered publish instances use
        // cases can be supported by only running the Subscriber
        // agent on the leader instance.
        processedOffsets = new LocalStore(resolverFactory, "packages", subAgentName);
        long startOffset = processedOffsets.load("offset", -1L) + 1;

        processedStatuses = new LocalStore(resolverFactory, "statuses", subAgentName);

        String assign = messagingProvider.assignTo(startOffset);

        packagePoller = messagingProvider.createPoller(
                topics.getPackageTopic(), 
                Reset.earliest, 
                assign,
                create(PackageMessage.class, this::handlePackageMessage));

        if (editable) {

            /*
             * We currently only support commands requiring editable mode.
             * As an optimisation, we don't register a poller for non
             * editable subscribers.
             *
             * When supporting commands independent from editable mode,
             * this optimisation will be removed.
             */

            commandPoller = messagingProvider.createPoller(
                    topics.getCommandTopic(),
                    Reset.earliest,
                    create(CommandMessage.class, this::handleCommandMessage));
        }


        startBackgroundThread(this::processQueueItems,
                format("Package Message Processor for Subscriber agent %s", subAgentName));

        sender = messagingProvider.createSender();

        int announceDelay = PropertiesUtil.toInteger(properties.get("announceDelay"), 10000);
        MessageSender<DiscoveryMessage> disSender = messagingProvider.createSender();
        announcer = new Announcer(subSlingId,
                subAgentName,
                topics.getDiscoveryTopic(),
                queueNames,
                disSender,
                processedOffsets,
                packageRetries,
                maxRetries,
                config.editable(),
                announceDelay
                );

        pkgType = requireNonNull(packageBuilder.getType());

        String msg = format("Started Subscriber agent %s at offset %s, subscribed to agent names %s with package builder %s editable %s maxRetries %s errorQueueEnabled %s",
                subAgentName,
                startOffset,
                queueNames,
                pkgType,
                config.editable(),
                maxRetries,
                errorQueueEnabled);
        LOG.info(msg);
        Dictionary<String, Object> props = createServiceProps(config);
        componentReg = context.registerService(DistributionAgent.class, this, props);
    }

    private Set<String> getNotEmpty(String[] agentNames) {
        return asList(agentNames).stream()
                .filter(StringUtils::isNotBlank)
                .collect(toSet());
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
        announcer.close();
        componentReg.unregister();
        IOUtils.closeQuietly(packagePoller);
        IOUtils.closeQuietly(commandPoller);
        running = false;
        String msg = String.format("Stopped Subscriber agent %s, subscribed to Publisher agent names %s with package builder %s",
                subAgentName,
                queueNames,
                pkgType);
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
                .filter(item -> isIn(queueName, item)).findFirst().orElse(null);
        return new SubQueue(queueName, head, packageRetries);
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
    public DistributionResponse execute(ResourceResolver resourceResolver,
                                        DistributionRequest request)
            throws DistributionException {
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
        if (! queueNames.contains(message.getPubAgentName())) {
            LOG.info(String.format("Skipping package for Publisher agent %s (not subscribed)", message.getPubAgentName()));
            return;
        }
        if (! pkgType.equals(message.getPkgType())) {
            LOG.warn(String.format("Skipping package with type %s", message.getPkgType()));
            return;
        }
        DistributionQueueItem queueItem = QueueItemFactory.fromPackage(info, message, true);
        while (!tryEnqueue(queueItem) && running) {
        }
    }

    /**
     * We block here if the buffer is full in order to limit the number of
	 * binary packages fetched in memory. Note that each queued item contains
	 * the binary package to be imported.
     */
	private boolean tryEnqueue(DistributionQueueItem queueItem) {
		try {
		    queueItemsBuffer.put(queueItem);
            distributionMetricsService.getItemsBufferSize().increment();
		    return true;
		} catch (InterruptedException ignore) {
			return false;
		}
	}

    private void processQueueItems() {
        for (;running;) {
            try {
                // send status stored in a previous run if exists
                sendStoredStatus();
                // We block until an item is available
                // and then process it
                DistributionQueueItem item = blockingPeekQueueItem();
                if (item != STOPPED_ITEM) {
                    processQueueItem(item);
                }
            } catch (Throwable t) {
                // Catch all to prevent processing from stopping
                LOG.error("Error processing queue item", t);
                sleep(RETRY_DELAY);
            }
        }
    }


    private DistributionQueueItem blockingPeekQueueItem() {
        for (;running;) {
            DistributionQueueItem queueItem = queueItemsBuffer.peek();
            if (queueItem != null) {
                return queueItem;
            } else {
                sleep(QUEUE_FETCH_DELAY);
            }
        }
        return STOPPED_ITEM;
    }

    private void processQueueItem(DistributionQueueItem queueItem) throws Exception {
        Timer.Context context = distributionMetricsService.getProcessQueueItemDuration().time();
        try {
            timedProcessQueueItem(queueItem);
        } finally {
            context.stop();
        }
    }

    private void timedProcessQueueItem(DistributionQueueItem queueItem) throws Exception {
        long offset = queueItem.get(RECORD_OFFSET, Long.class);
        boolean skip;
        try {
            skip = isCleared(offset) || cannotProcess(offset);
        } catch (IllegalStateException e) {
            /**
             * This will occur when the precondition times out.
             */
            LOG.info(e.getMessage());
            sleep(RETRY_DELAY);
            return;
        }
        PackageMessage pkgMsg = queueItem.get(PACKAGE_MSG, PackageMessage.class);
        String pubAgentName = pkgMsg.getPubAgentName();
        if (skip) {
            removePackage(pkgMsg, offset);
        } else {
            long createdTime = queueItem.get(RECORD_TIMESTAMP, Long.class);
            importPackage(pkgMsg, offset, createdTime);
        }
        queueItemProcessed(pubAgentName);
    }

    private void removePackage(PackageMessage pkgMsg, long offset) throws Exception {
        LOG.info(format("Removing distribution package %s of type %s at offset %s", pkgMsg.getPkgId(), pkgMsg.getReqType(), offset));
        Timer.Context context = distributionMetricsService.getRemovedPackageDuration().time();
        try (ResourceResolver resolver = getServiceResolver("bookkeeper")) {
            if (editable) {
                storeStatus(resolver, REMOVED, offset, pkgMsg.getPubAgentName());
            }
            storeOffset(resolver, offset);
            resolver.commit();
            context.stop();
        }
    }

    private void removeFailedPackage(PackageMessage pkgMsg, long offset) throws Exception {
        LOG.info(format("Removing failed distribution package %s of type %s at offset %s", pkgMsg.getPkgId(), pkgMsg.getReqType(), offset));
        Timer.Context context = distributionMetricsService.getRemovedFailedPackageDuration().time();
        try (ResourceResolver resolver = getServiceResolver("bookkeeper")) {
            storeStatus(resolver, REMOVED_FAILED, offset, pkgMsg.getPubAgentName());
            storeOffset(resolver, offset);
            resolver.commit();
            context.stop();
        }
    }

    private void importPackage(PackageMessage pkgMsg, long offset, long createdTime)
            throws Exception {
        String pubAgentName = pkgMsg.getPubAgentName();
        LOG.info(format("Importing distribution package %s of type %s at offset %s", pkgMsg.getPkgId(), pkgMsg.getReqType(), offset));
        addPackageMDC(pkgMsg);
        Timer.Context context = distributionMetricsService.getImportedPackageDuration().time();
        try (ResourceResolver importerResolver = getServiceResolver("importer")) {

            /*
             * We aim at processing the packages exactly once.
             * Processing the packages exactly once is possible
             * with the following conditions
             *
             * I.  The package importer is configured to disable
             *     auto-committing changes.
             *
             * II. A single commit aggregates three content updates
             *
             *     C1. install the package
             *     C2. store the processing status
             *     C3. store the offset processed
             *
             * Some package importers require auto-saving or issue
             * partial commits before failing.
             * For those packages importers, we aim at processing
             * packages at least once, thanks to the order in which
             * the content updates are applied.
             */

            installPackage(importerResolver, pkgMsg);
            if (editable) {
                storeStatus(importerResolver, IMPORTED, offset, pubAgentName);
            }
            storeOffset(importerResolver, offset);
            importerResolver.commit();

            context.stop();
            distributionMetricsService.getImportedPackageSize().update(pkgMsg.getPkgLength());
            distributionMetricsService.getPackageDistributedDuration().update((currentTimeMillis() - createdTime), TimeUnit.MILLISECONDS);

            Event event = DistributionEvent.eventImporterImported(pkgMsg, subAgentName);
            eventAdmin.postEvent(event);
        } catch (Throwable e) {
            distributionMetricsService.getFailedPackageImports().mark();
            // rethrow fatal exceptions
            if (e instanceof VirtualMachineError) {
                throw (VirtualMachineError) e;
            }
            int retries = packageRetries.get(pubAgentName);
            if (errorQueueEnabled && retries >= maxRetries) {
                LOG.warn(format("Failed to import distribution package %s at offset %s after %s retries, removing the package.", pkgMsg.getPkgId(), offset, retries));
                removeFailedPackage(pkgMsg, offset);
            } else {
                packageRetries.increase(pubAgentName);
                String msg = format("Error processing distribution package %s. Retry attempts %s/%s.", pkgMsg.getPkgId(), retries, errorQueueEnabled ? Integer.toString(maxRetries) : "infinite");
                throw new DistributionException(msg, e);
            }
        } finally {
            MDC.clear();
        }
    }

    private void storeOffset(ResourceResolver importerResolver, long offset)
            throws PersistenceException {
        processedOffsets.store(importerResolver, "offset", offset);
    }

    private void queueItemProcessed(String pubAgentName) {
        packageRetries.clear(pubAgentName);
        queueItemsBuffer.remove();
        distributionMetricsService.getItemsBufferSize().decrement();
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


    private void installPackage(ResourceResolver resolver, PackageMessage pkgMsg)
            throws DistributionException, PersistenceException {
        PackageMessage.ReqType type = pkgMsg.getReqType();
        switch (type) {
            case ADD:
                installAddPackage(resolver, pkgMsg);
                break;
            case DELETE:
                installDeletePackage(resolver, pkgMsg);
                break;
            case TEST:
                break;
            default: throw new UnsupportedOperationException(format("Unable to process messages with type: %s", type));
        }
    }

    private void installAddPackage(ResourceResolver resolver, PackageMessage pkgMsg)
            throws DistributionException {
        LOG.info("Importing paths " + pkgMsg.getPathsList());
        InputStream pkgStream = null;
        try {
            pkgStream = pkgStream(resolver, pkgMsg);
            packageBuilder.installPackage(resolver, pkgStream);
        } finally {
            IOUtils.closeQuietly(pkgStream);
        }

    }

    private void installDeletePackage(ResourceResolver resolver, PackageMessage pkgMsg)
            throws PersistenceException {
        LOG.info("Deleting paths " + pkgMsg.getPathsList());
        for (String path : pkgMsg.getPathsList()) {
            Resource resource = resolver.getResource(path);
            if (resource != null) {
                resolver.delete(resource);
            }
        }
    }

    private void storeStatus(ResourceResolver resolver, Status status, long offset, String pubAgentName) throws PersistenceException {
        Map<String, Object> s = new HashMap<>();
        s.put("pubAgentName", pubAgentName);
        s.put("statusNumber", status.getNumber());
        s.put("offset", offset);
        s.put("sent", false);
        processedStatuses.store(resolver, s);
        LOG.info("Stored status {}", s);
    }

    private void sendStoredStatus() {
        Timer.Context context = distributionMetricsService.getSendStoredStatusDuration().time();
        try {
            timedSendStoredStatus();
        } finally {
            context.stop();
        }
    }

    private void timedSendStoredStatus() {
        ValueMap status = processedStatuses.load();
        boolean sent = status.get("sent", true);
        for (int retry = 0 ; running && !sent ; retry++) {
            try {
                sendStatusMessage(status);
                sent = true;
            } catch (MessagingException e) {
                LOG.warn("Cannot send status (retry {})", retry, e);
                sleep(RETRY_SEND_DELAY);
            }
        }
        markStatusSent();
    }

    private void markStatusSent() {
        try (ResourceResolver resolver = getServiceResolver("bookkeeper")) {
            processedStatuses.store(resolver, "sent", true);
            resolver.commit();
        } catch (Exception e) {
            LOG.warn("Failed to mark status as sent", e);
        }
    }

    private void sendStatusMessage(ValueMap status) throws MessagingException {

        PackageStatusMessage pkgStatMsg = PackageStatusMessage.newBuilder()
                .setSubSlingId(subSlingId)
                .setSubAgentName(subAgentName)
                .setPubAgentName(status.get("pubAgentName", String.class))
                .setOffset(status.get("offset", Long.class))
                .setStatus(Status.valueOf(status.get("statusNumber", Integer.class)))
                .build();

        sender.send(topics.getStatusTopic(), pkgStatMsg);
        LOG.info("Sent status message {}", status);
    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignore) {
            // ignore
        }
    }

    @Nonnull
    private InputStream pkgStream(ResourceResolver resolver, PackageMessage pkgMsg) throws DistributionException {
        if (pkgMsg.hasPkgBinary()) {
            return new ByteArrayInputStream(pkgMsg.getPkgBinary().toByteArray());
        } else {
            String pkgBinRef = pkgMsg.getPkgBinaryRef();
            try {
                ValueFactory factory = resolver.adaptTo(Session.class).getValueFactory();
                Binary binary = factory.createValue(new SimpleReferenceBinary(pkgBinRef)).getBinary();
                return binary.getStream();
            } catch (RepositoryException e) {
                throw new DistributionException(e.getMessage(), e);
            }
        }
    }

    private void handleCommandMessage(MessageInfo info, CommandMessage message) {
        if (subSlingId.equals(message.getSubSlingId()) && subAgentName.equals(message.getSubAgentName())) {
            if (message.hasClearCommand()) {
                handleClearCommand(message.getClearCommand().getOffset());
            } else {
                LOG.warn("Unsupported command {}", message);
            }
        } else {
            LOG.debug(format("Skip command for subSlingId %s", message.getSubSlingId()));
        }
    }

    private boolean isCleared(long offset) {
        return offset <= clearOffset.longValue();
    }

    private boolean cannotProcess(long offset) {
	    return !precondition.canProcess(offset , PRECONDITION_TIMEOUT);
    }

    private void handleClearCommand(long offset) {
        if (editable) {
            // atomically compare and set clearOffset
            // as the max between the provided offset
            // and the current clearOffset
            clearOffset.accumulateAndGet(offset, Math::max);
        } else {
            LOG.warn("Unexpected ClearCommand for non editable subscriber");
        }

    }

    private ResourceResolver getServiceResolver(String subService) throws LoginException {
        return resolverFactory.getServiceResourceResolver(singletonMap(SUBSERVICE, subService));
    }

}
