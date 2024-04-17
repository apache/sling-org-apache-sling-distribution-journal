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
package org.apache.sling.distribution.journal.impl.publisher;


import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.sling.distribution.DistributionRequestState.ACCEPTED;
import static org.apache.sling.distribution.DistributionRequestType.*;
import static org.apache.sling.distribution.journal.shared.Strings.requireNotBlank;
import static org.osgi.service.component.annotations.ReferenceCardinality.OPTIONAL;
import static org.osgi.service.component.annotations.ReferencePolicyOption.GREEDY;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.impl.discovery.DiscoveryService;
import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.shared.DefaultDistributionLog;
import org.apache.sling.distribution.journal.shared.DistributionLogEventListener;
import org.apache.sling.distribution.journal.shared.Timed;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestState;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.log.spi.DistributionLog;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.condition.Condition;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.metatype.annotations.Designate;
import org.apache.sling.distribution.journal.MessagingProvider;

/**
 * A Publisher SCD agent which produces messages to be consumed by a {@code DistributionSubscriber} agent.
 */
@Component(
        immediate = true,
        configurationPid = DistributionPublisher.FACTORY_PID
)
@Designate(ocd = PublisherConfiguration.class, factory = true)
@ParametersAreNonnullByDefault
public class DistributionPublisher implements DistributionAgent {

    public static final String FACTORY_PID = "org.apache.sling.distribution.journal.impl.publisher.DistributionPublisherFactory";

    @Nonnull
    private final DefaultDistributionLog distLog;

    private final DistributionPackageBuilder packageBuilder;

    private final PackageMessageFactory factory;

    private final EventAdmin eventAdmin;

    private final PublishMetrics publishMetrics;

    private final PubQueueProvider pubQueueProvider;

    private final String pubAgentName;

    private final String pkgType;

    private final boolean limitEnabled;

    private final long queuedTimeout;

    private final int queueSizeLimit;

    private final int maxQueueSizeDelay;

    private final Consumer<PackageMessage> sender;

    private final DistributionLogEventListener distributionLogEventListener;

    @Activate
    public DistributionPublisher(
            @Reference
            MessagingProvider messagingProvider,
            @Reference(name = "packageBuilder")
            DistributionPackageBuilder packageBuilder,
            @Reference
            DiscoveryService discoveryService,
            @Reference
            PackageMessageFactory factory,
            @Reference
            EventAdmin eventAdmin,
            @Reference
            Topics topics,
            @Reference
            MetricsService metricsService,
            @Reference
            PubQueueProvider pubQueueProvider,
            @Reference(target = "(osgi.condition.id=toggle.FT_SLING-12218)", cardinality = OPTIONAL, policyOption = GREEDY)
            Condition limitToggle,
            PublisherConfiguration config,
            BundleContext context) {

        pubAgentName = requireNotBlank(config.name());

        this.packageBuilder = packageBuilder;
        this.factory = requireNonNull(factory);
        this.eventAdmin = eventAdmin;
        requireNonNull(metricsService);
        this.publishMetrics = new PublishMetrics(metricsService, pubAgentName);
        this.pubQueueProvider = pubQueueProvider;
        this.publishMetrics.queueSize(() -> pubQueueProvider.getMaxQueueSize(pubAgentName));

        distLog = new DefaultDistributionLog(pubAgentName, this.getClass(), DefaultDistributionLog.LogLevel.INFO);
        distributionLogEventListener = new DistributionLogEventListener(context, distLog, pubAgentName);

        limitEnabled = limitToggle != null;
        queuedTimeout = config.queuedTimeout();
        queueSizeLimit = config.queueSizeLimit();
        maxQueueSizeDelay = config.maxQueueSizeDelay();
        pkgType = packageBuilder.getType();

        this.sender = messagingProvider.createSender(topics.getPackageTopic());
        publishMetrics.subscriberCount(() -> discoveryService.getSubscriberCount(pubAgentName));
        
        distLog.info("Started Publisher agent={} with packageBuilder={}, limitEnabled={}, queuedTimeout={}, queueSizeLimit={}, maxQueueSizeDelay={}",
                pubAgentName, pkgType, limitEnabled, queuedTimeout, queueSizeLimit, maxQueueSizeDelay);
    }

    @Deactivate
    public void deactivate() {
        IOUtils.closeQuietly(distributionLogEventListener);
        String msg = format("Stopped Publisher agent %s with packageBuilder %s, queuedTimeout %s",
                pubAgentName, pkgType, queuedTimeout);
        distLog.info(msg);
    }
    
    /**
     * Get queue names for alive subscribed subscriber agents.
     */
    @SuppressWarnings("null")
    @Nonnull
    @Override
    public Iterable<String> getQueueNames() {
        return Collections.unmodifiableCollection(pubQueueProvider.getQueueNames(pubAgentName));
    }

    @Override
    public DistributionQueue getQueue(String queueName) {
        try {
            DistributionQueue queue = pubQueueProvider.getQueue(pubAgentName, queueName);
            if (queue == null) {
                publishMetrics.getQueueAccessErrorCount().increment();
            }
            return queue;
        } catch (Exception e) {
            publishMetrics.getQueueAccessErrorCount().increment();
            throw e;
        }
    }

    @Nonnull
    @Override
    public DistributionLog getLog() {
        return distLog;
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
        if (request.getRequestType() == PULL) {
            String msg = "Request requestType=PULL not supported by this agent";
            distLog.info(msg);
            return new SimpleDistributionResponse(DistributionRequestState.DROPPED, msg);
        }
        int queueSize = pubQueueProvider.getMaxQueueSize(pubAgentName);
        int sleepMs = getSleepTime(queueSize);
        sleep(sleepMs);
        final PackageMessage pkg = buildPackage(resourceResolver, request);
        return send(pkg, queueSize, sleepMs);
    }

    int getSleepTime(int queueSize) {
        if (!limitEnabled || queueSize <= queueSizeLimit) {
            return 0;
        } else if (queueSize >= queueSizeLimit*2) {
            return maxQueueSizeDelay;
        } else {
            return (queueSize-queueSizeLimit) * maxQueueSizeDelay / queueSizeLimit;
        }
    }

    private void sleep(long sleepMs) throws DistributionException {
        if (sleepMs <= 0) {
            return;
        }
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DistributionException("Interrupted");
        }
    }

    private PackageMessage buildPackage(ResourceResolver resourceResolver, DistributionRequest request)
            throws DistributionException {
        try {
            if (request.getRequestType() != TEST && request.getPaths().length == 0) {
                throw new DistributionException("Empty paths are not allowed");
            }
            return Timed.timed(publishMetrics.getBuildPackageDuration(), () -> factory.create(packageBuilder, resourceResolver, pubAgentName, request));
        } catch (Exception e) {
            publishMetrics.getDroppedRequests().mark();
            String msg = format("Failed to create content package for requestType=%s, paths=%s. Error=%s",
                    request.getRequestType(), Arrays.toString(request.getPaths()), e.getMessage());
            distLog.error(msg, e);
            throw new DistributionException(msg, e);
        }
    }
    
    @Nonnull
    private DistributionResponse send(final PackageMessage pkg, int queueSize, int delayMS) throws DistributionException {
        try {
            long offset = Timed.timed(publishMetrics.getEnqueuePackageDuration(), () -> this.sendAndWait(pkg));
            publishMetrics.getExportedPackageSize().update(pkg.getPkgLength());
            publishMetrics.getAcceptedRequests().mark();
            String msg = format("Request accepted with distribution package %s at offset=%d, queueSize=%d, queueSizeDelay=%d", pkg, offset, queueSize, delayMS);
            distLog.info(msg);
            return new SimpleDistributionResponse(ACCEPTED, msg, pkg::getPkgId);
        } catch (Throwable e) {
            publishMetrics.getDroppedRequests().mark();
            String msg = format("Failed to append distribution package %s to the journal", pkg);
            distLog.error(msg, e);
            if (e instanceof Error) {
                throw (Error) e;
            } else {
                throw new DistributionException(msg, e);
            }
        }
    }

    private long sendAndWait(PackageMessage pkg) {
        if (pkg.getReqType() == ReqType.TEST) {
            // Do not wait in case of TEST as we do not actually send it out
            sender.accept(pkg);
            return -1;
        }
        PackageQueuedNotifier queuedNotifier = pubQueueProvider.getQueuedNotifier();
        try {
            CompletableFuture<Long> received = queuedNotifier.registerWait(pkg.getPkgId());
            Event createdEvent = DistributionEvent.eventPackageCreated(pkg, pubAgentName);
            eventAdmin.postEvent(createdEvent);
            sender.accept(pkg);
            return received.get(queuedTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            queuedNotifier.unRegisterWait(pkg.getPkgId());
            throw new RuntimeException(e);
        }
    }

}
