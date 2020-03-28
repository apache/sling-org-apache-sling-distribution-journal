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
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.sling.distribution.journal.HandlerAdapter.create;
import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;

import java.io.Closeable;
import java.util.Collections;
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
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.precondition.Precondition;
import org.apache.sling.distribution.journal.impl.precondition.Precondition.Decision;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
import org.apache.sling.distribution.journal.service.subscriber.BookKeeper;
import org.apache.sling.distribution.journal.service.subscriber.BookKeeperFactory;
import org.apache.sling.distribution.journal.service.subscriber.PackageHandling;
import org.apache.sling.distribution.journal.service.subscriber.SubscriberMetrics;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.settings.SlingSettingsService;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

/**
 * A Subscriber SCD agent which consumes messages produced by a
 * {@code DistributionPublisher} agent.
 */
@Component(service = DistributionSubscriber.class, immediate = true, 
    property = { "announceDelay=10000" }, 
    configurationPid = "org.apache.sling.distribution.journal.impl.subscriber.DistributionSubscriberFactory")
@Designate(ocd = SubscriberConfiguration.class, factory = true)
@ParametersAreNonnullByDefault
public class DistributionSubscriber {
    private static final int PRECONDITION_TIMEOUT = 60;
    static int RETRY_DELAY = 5000;
    static int QUEUE_FETCH_DELAY = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(DistributionSubscriber.class);

    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    @Reference
    private JournalAvailable journalAvailable;

    @Reference(name = "precondition")
    private Precondition precondition;

    @Reference(name = "packageBuilder")
    private DistributionPackageBuilder packageBuilder;

    @Reference
    private SubscriberMetrics subscriberMetrics;
    
    @Reference
    private SlingSettingsService slingSettings;
    
    @Reference
    BookKeeperFactory bookKeeperFactory;

    private ServiceRegistration<DistributionAgent> componentReg;

    private Closeable packagePoller;

    private CommandPoller commandPoller;

    BookKeeper bookKeeper;
    
    // Use a bounded internal buffer to allow reading further packages while working
    // on one at a time
    private final BlockingQueue<FullMessage<PackageMessage>> queueItemsBuffer = new LinkedBlockingQueue<>(8);

    private Set<String> queueNames = Collections.emptySet();

    private Announcer announcer;

    private String subAgentName;

    private volatile boolean running = true;

    private volatile Thread queueProcessor;

    @Activate
    public void activate(SubscriberConfiguration config, BundleContext context, Map<String, Object> properties) {
        subAgentName = requireNonNull(config.name());
        requireNonNull(config);
        requireNonNull(context);

        requireNonNull(packageBuilder);
        requireNonNull(subscriberMetrics);
        requireNonNull(slingSettings);
        requireNonNull(messagingProvider);
        requireNonNull(topics);
        requireNonNull(precondition);
        requireNonNull(bookKeeperFactory);

        queueNames = getNotEmpty(config.agentNames());
        String subSlingId = requireNonNull(slingSettings.getSlingId());
        int maxRetries = config.maxRetries();
        boolean editable = config.editable();
        PackageHandling packageHandling = config.packageHandling();
        bookKeeper = bookKeeperFactory.create(packageBuilder, subAgentName, subSlingId, maxRetries, editable, packageHandling, sender(topics.getStatusTopic()));
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

        boolean errorQueueEnabled = (maxRetries >= 0);
        String msg = format(
                "Started Subscriber agent %s at offset %s, subscribed to agent names %s editable %s maxRetries %s errorQueueEnabled %s",
                subAgentName, startOffset, queueNames, config.editable(), maxRetries, errorQueueEnabled);
        LOG.info(msg);
    }

    private <T extends GeneratedMessage> Consumer<T> sender(String topic) {
        MessageSender<T> sender = messagingProvider.createSender();
        return msg -> sender.send(topic, msg);
    }

    private Set<String> getNotEmpty(String[] agentNames) {
        return asList(agentNames).stream().filter(StringUtils::isNotBlank).collect(toSet());
    }

    @Deactivate
    public void deactivate() {
        componentReg.unregister();
        IOUtils.closeQuietly(announcer, bookKeeper, 
                packagePoller, commandPoller);
        running = false;
        Thread interrupter = this.queueProcessor;
        if (interrupter != null) {
            interrupter.interrupt();
        }
        String msg = String.format(
                "Stopped Subscriber agent %s, subscribed to Publisher agent names %s",
                subAgentName, queueNames);
        LOG.info(msg);
    }
    
    @Nonnull
    public DistributionAgentState getState() {
        return bookKeeper.getState();
    }
    
    private void handlePackageMessage(MessageInfo info, PackageMessage message) {
        if (shouldEnqueue(info, message)) {
            FullMessage<PackageMessage> queueItem = new FullMessage<PackageMessage>(info, message);
            enqueue(queueItem);
        } else {
            bookKeeper.skipPackage(info.getOffset());
        }
    }

    private boolean shouldEnqueue(MessageInfo info, PackageMessage message) {
        if (!queueNames.contains(message.getPubAgentName())) {
            LOG.info("Skipping package for Publisher agent {} at offset {} (not subscribed)", message.getPubAgentName(), info.getOffset());
            return false;
        }
        return true;
    }
    
    /**
     * We block here if the buffer is full in order to limit the number of binary
     * packages fetched in memory. Note that each queued item contains the binary
     * package to be imported.
     */
    private void enqueue(FullMessage<PackageMessage> queueItem) {
        try {
            while (running) {
                if (queueItemsBuffer.offer(queueItem, 1000, TimeUnit.MILLISECONDS)) {
                    subscriberMetrics.getItemsBufferSize().increment();
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
            FullMessage<PackageMessage> item = blockingPeekQueueItem();
            try (Timer.Context context = subscriberMetrics.getProcessQueueItemDuration().time()) {
                bookKeeper.processPackage(item.getInfo(), item.getMessage(), this::shouldRemove);
            }
            queueItemsBuffer.remove();
        } catch (PreConditionTimeoutException e) {
            LOG.info(e.getMessage());
            retryDelay();
        } catch (Exception e) {
            // Catch all to prevent processing from stopping
            LOG.warn("Error processing queue item", e);
            retryDelay();
        }
    }
    
    private FullMessage<PackageMessage> blockingPeekQueueItem() throws InterruptedException {
        while (true) {
            FullMessage<PackageMessage> queueItem = queueItemsBuffer.peek();
            if (queueItem != null) {
                return queueItem;
            } else {
                Thread.sleep(QUEUE_FETCH_DELAY);
            }
        }
    }

    private void retryDelay() {
        try {
            Thread.sleep(RETRY_DELAY);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private boolean shouldRemove(long offset, PackageMessage message) {
        if (commandPoller.isCleared(offset)) {
            return true;
        }
       return waitPrecondition(offset);
    }

    private boolean waitPrecondition(long offset) {
        Decision decision = Decision.WAIT;
        long endTime = System.currentTimeMillis() + PRECONDITION_TIMEOUT * 1000;
        while (decision == Decision.WAIT && System.currentTimeMillis() < endTime) {
            decision = precondition.canProcess(subAgentName, offset);
            if (decision == Decision.WAIT) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            } else {
                return decision == Decision.SKIP ? true : false;
            }
        }
        throw new PreConditionTimeoutException("Timeout waiting for package offset " + offset + " on status topic.");
    }

}
