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
import static org.apache.sling.distribution.journal.shared.DistributionMetricsService.timed;
import static org.apache.sling.distribution.journal.shared.Strings.requireNotBlank;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.management.NotCompliantMBeanException;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.impl.discovery.DiscoveryService;
import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.shared.DefaultDistributionLog;
import org.apache.sling.distribution.journal.shared.DistributionLogEventListener;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.JMXRegistration;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.api.resource.ResourceResolver;
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
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.metatype.annotations.Designate;

import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.HandlerAdapter;

/**
 * A Publisher SCD agent which produces messages to be consumed by a {@code DistributionSubscriber} agent.
 */
@Component(
        service = {}, 
        immediate = true,
        configurationPid = DistributionPublisher.FACTORY_PID
)
@Designate(ocd = PublisherConfiguration.class, factory = true)
@ParametersAreNonnullByDefault
public class DistributionPublisher implements DistributionAgent {

    public static final String FACTORY_PID = "org.apache.sling.distribution.journal.impl.publisher.DistributionPublisherFactory";

    @Nonnull
    private final DefaultDistributionLog log;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference(name = "packageBuilder")
    private DistributionPackageBuilder packageBuilder;

    @Reference
    private DiscoveryService discoveryService;

    @Reference
    private PackageMessageFactory factory;

    @Reference
    private EventAdmin eventAdmin;

    @Reference
    private Topics topics;
    
    @Reference
    private DistributionMetricsService distributionMetricsService;

    @Reference
    private PubQueueProvider pubQueueProvider;

    private String pubAgentName;

    private String pkgType;

    private long queuedTimeout;

    private ServiceRegistration<DistributionAgent> componentReg;

    private Consumer<PackageMessage> sender;

    private JMXRegistration reg;

    private Closeable statusPoller;

    private DistributionLogEventListener distributionLogEventListener;


    public DistributionPublisher() {
        log = new DefaultDistributionLog(pubAgentName, this.getClass(), DefaultDistributionLog.LogLevel.INFO);
    }

    @Activate
    public void activate(PublisherConfiguration config, BundleContext context) {
        requireNonNull(factory);
        requireNonNull(distributionMetricsService);
        pubAgentName = requireNotBlank(config.name());

        queuedTimeout = config.queuedTimeout();

        pkgType = packageBuilder.getType();

        this.sender = messagingProvider.createSender(topics.getPackageTopic());
        
        Dictionary<String, Object> props = createServiceProps(config);
        componentReg = requireNonNull(context.registerService(DistributionAgent.class, this, props));
        
        distributionLogEventListener = new DistributionLogEventListener(context, log, pubAgentName);

        DistPublisherJMX bean;
        try {
            bean = new DistPublisherJMX(pubAgentName, discoveryService, this);
        } catch (NotCompliantMBeanException e) {
            throw new RuntimeException(e);
        }
        reg = new JMXRegistration(bean, "agent", pubAgentName);
        
        String msg = format("Started Publisher agent %s with packageBuilder %s, queuedTimeout %s",
                pubAgentName, pkgType, queuedTimeout);
        distributionMetricsService.createGauge(
                DistributionMetricsService.PUB_COMPONENT + ".subscriber_count;pub_name=" + pubAgentName,
                () -> discoveryService.getTopologyView().getSubscribedAgentIds().size()
        );
        
        statusPoller = messagingProvider.createPoller(
                topics.getStatusTopic(),
                Reset.earliest,
                HandlerAdapter.create(PackageStatusMessage.class, pubQueueProvider::handleStatus)
                );
        
        log.info(msg);
    }

    @Deactivate
    public void deactivate() {
        IOUtils.closeQuietly(statusPoller, distributionLogEventListener, reg);
        componentReg.unregister();
        String msg = format("Stopped Publisher agent %s with packageBuilder %s, queuedTimeout %s",
                pubAgentName, pkgType, queuedTimeout);
        log.info(msg);
    }
    
    private Dictionary<String, Object> createServiceProps(PublisherConfiguration config) {
        Dictionary<String, Object> props = new Hashtable<>();
        props.put("name", config.name());
        props.put("title", config.name());
        props.put("details", config.name());
        props.put("packageBuilder.target", config.packageBuilder_target());
        props.put("webconsole.configurationFactory.nameHint", config.webconsole_configurationFactory_nameHint());
        return props;
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
                distributionMetricsService.getQueueAccessErrorCount().increment();
            }
            return queue;
        } catch (Exception e) {
            distributionMetricsService.getQueueAccessErrorCount().increment();
            throw e;
        }
    }

    @Nonnull
    @Override
    public DistributionLog getLog() {
        return log;
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
            log.info(msg);
            return new SimpleDistributionResponse(DistributionRequestState.DROPPED, msg);
        }
        final PackageMessage pkg = buildPackage(resourceResolver, request);
        return sendPackageMessage(pkg);
    }

    private PackageMessage buildPackage(ResourceResolver resourceResolver, DistributionRequest request)
            throws DistributionException {
        try {
            if (request.getRequestType() != TEST && request.getPaths().length == 0) {
                throw new DistributionException("Empty paths are not allowed");
            }
            return timed(distributionMetricsService.getBuildPackageDuration(), () -> factory.create(packageBuilder, resourceResolver, pubAgentName, request));
        } catch (Exception e) {
            distributionMetricsService.getDroppedRequests().mark();
            String msg = format("Failed to create content package for requestType=%s, paths=%s. Error=%s",
                    request.getRequestType(), Arrays.toString(request.getPaths()), e.getMessage());
            log.error(msg, e);
            throw new DistributionException(msg, e);
        }
    }
    
    @Nonnull
    private DistributionResponse sendPackageMessage(final PackageMessage pkg) throws DistributionException {
        try {
            long offset = timed(distributionMetricsService.getEnqueuePackageDuration(), () -> this.sendAndWait(pkg));
            distributionMetricsService.getExportedPackageSize().update(pkg.getPkgLength());
            distributionMetricsService.getAcceptedRequests().mark();
            String msg = format("Request accepted with distribution package %s at offset=%s", pkg, offset);
            log.info(msg);
            return new SimpleDistributionResponse(ACCEPTED, msg, pkg::getPkgId);
        } catch (Throwable e) {
            distributionMetricsService.getDroppedRequests().mark();
            String msg = format("Failed to append distribution package %s to the journal", pkg);
            log.error(msg, e);
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
