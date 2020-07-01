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


import static java.util.stream.StreamSupport.stream;
import static java.util.Objects.requireNonNull;
import static org.apache.sling.distribution.DistributionRequestState.ACCEPTED;
import static org.apache.sling.distribution.DistributionRequestType.ADD;
import static org.apache.sling.distribution.DistributionRequestType.DELETE;
import static org.apache.sling.distribution.DistributionRequestType.TEST;
import static org.apache.sling.distribution.journal.shared.DistributionMetricsService.timed;

import java.util.Arrays;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.management.NotCompliantMBeanException;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.sling.distribution.journal.impl.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.shared.AgentState;
import org.apache.sling.distribution.journal.shared.DefaultDistributionLog;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.JMXRegistration;
import org.apache.sling.distribution.journal.shared.SimpleDistributionResponse;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestState;
import org.apache.sling.distribution.DistributionRequestType;
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
import org.apache.sling.distribution.journal.JournalAvailable;

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

    private final Map<DistributionRequestType, Consumer<PackageMessage>> REQ_TYPES = new HashMap<>();

    private final DefaultDistributionLog log;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference(name = "packageBuilder")
    private DistributionPackageBuilder packageBuilder;

    @Reference
    private PackageQueuedNotifier queuedNotifier;

    @Reference
    private PubQueueProvider pubQueueProvider;

    @Reference
    private DiscoveryService discoveryService;

    @Reference
    private PackageMessageFactory factory;

    @Reference
    private EventAdmin eventAdmin;

    @Reference
    private Topics topics;
    
    @Reference
    JournalAvailable journalAvailable;

    @Reference
    private DistributionMetricsService distributionMetricsService;

    private String pubAgentName;

    private String pkgType;

    private long queuedTimeout;

    private ServiceRegistration<DistributionAgent> componentReg;

    private Consumer<PackageMessage> sender;

    private JMXRegistration reg;

    private DistributionMetricsService.GaugeService<Integer> subscriberCountGauge;

    public DistributionPublisher() {
        log = new DefaultDistributionLog(pubAgentName, this.getClass(), DefaultDistributionLog.LogLevel.INFO);
        REQ_TYPES.put(ADD,    this::sendAndWait);
        REQ_TYPES.put(DELETE, this::sendAndWait);
        REQ_TYPES.put(TEST,   this.sender);
    }

    @Activate
    public void activate(PublisherConfiguration config, BundleContext context) {
        requireNonNull(factory);
        requireNonNull(distributionMetricsService);
        pubAgentName = requireNonNull(config.name());

        queuedTimeout = config.queuedTimeout();

        pkgType = packageBuilder.getType();

        this.sender = messagingProvider.createSender(topics.getPackageTopic());
        
        Dictionary<String, Object> props = createServiceProps(config);
        componentReg = requireNonNull(context.registerService(DistributionAgent.class, this, props));

        DistPublisherJMX bean;
        try {
            bean = new DistPublisherJMX(pubAgentName, discoveryService, this);
        } catch (NotCompliantMBeanException e) {
            throw new RuntimeException(e);
        }
        reg = new JMXRegistration(bean, "agent", pubAgentName);
        
        String msg = String.format("Started Publisher agent %s with packageBuilder %s, queuedTimeout %s",
                pubAgentName, pkgType, queuedTimeout);
        subscriberCountGauge = distributionMetricsService.createGauge(
                DistributionMetricsService.PUB_COMPONENT + ".subscriber_count;pub_name=" + pubAgentName,
                "Current number of publish subscribers",
                () -> discoveryService.getTopologyView().getSubscribedAgentIds().size()
        );
        log.info(msg);
    }

    @Deactivate
    public void deactivate() {
        reg.close();
        componentReg.unregister();
        String msg = String.format("Stopped Publisher agent %s with packageBuilder %s, queuedTimeout %s",
                pubAgentName, pkgType, queuedTimeout);
        IOUtils.closeQuietly(subscriberCountGauge);
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
    @Nonnull
    @Override
    public Iterable<String> getQueueNames() {

        // Queues names are generated only for the subscriber agents which are
        // alive and are subscribed to the publisher agent name (pubAgentName).
        // The queue names match the subscriber agent identifier (subAgentId).
        //
        // If errors queues are enabled, an error queue name is generated which
        // follows the pattern "%s-error". The pattern is deliberately different
        // from the SCD on Jobs one ("error-%s") as we don't want to support
        // the UI ability to retry items from the error queue.
        Set<String> queueNames = new HashSet<>();
        TopologyView view =  discoveryService.getTopologyView();
        for (String subAgentId : view.getSubscribedAgentIds(pubAgentName)) {
            queueNames.add(subAgentId);
            State subState = view.getState(subAgentId, pubAgentName);
            if (subState != null) {
                boolean errorQueueEnabled = (subState.getMaxRetries() >= 0);
                if (errorQueueEnabled) {
                    queueNames.add(String.format("%s-error", subAgentId));
                }
            }
        }
        return Collections.unmodifiableCollection(queueNames);
    }

    @Override
    public DistributionQueue getQueue(String queueName) {

        // validate that queueName is a valid name returned by #getQueueNames
        if (stream(getQueueNames().spliterator(), true).noneMatch(queueName::equals)) {
            distributionMetricsService.getQueueAccessErrorCount().increment();
            return null;
        }

        try {
            return queueName.endsWith("-error") ? getErrorQueue(queueName) : getPubQueue(queueName);
        } catch (Exception e) {
            distributionMetricsService.getQueueAccessErrorCount().increment();
            throw e;
        }
    }

    @Nonnull
    private DistributionQueue getErrorQueue(String queueName) {
        AgentId subAgentId = new AgentId(StringUtils.substringBeforeLast(queueName, "-error"));
        return pubQueueProvider.getErrorQueue(pubAgentName, subAgentId.getSlingId(), subAgentId.getAgentName(), queueName);
    }

    @CheckForNull
    private DistributionQueue getPubQueue(String queueName) {
        TopologyView view = discoveryService.getTopologyView();
        AgentId subAgentId = new AgentId(queueName);
        State state = view.getState(subAgentId.getAgentId(), pubAgentName);
        if (state != null) {
            return pubQueueProvider.getQueue(pubAgentName, subAgentId.getSlingId(), subAgentId.getAgentName(), queueName, state.getOffset() + 1, state.getRetries(), state.isEditable());
        }
        return null;
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
        Consumer<PackageMessage> handler = REQ_TYPES.get(request.getRequestType());
        if (handler != null) {
            return execute(resourceResolver, request, handler);
        } else {
            return executeUnsupported(request);
        }
    }

    private DistributionResponse execute(ResourceResolver resourceResolver,
                                         DistributionRequest request,
                                         Consumer<PackageMessage> sender)
            throws DistributionException {
        try {
            PackageMessage pkg = timed(distributionMetricsService.getBuildPackageDuration(),
                    () -> factory.create(packageBuilder, resourceResolver, pubAgentName, request)
            );
            timed(distributionMetricsService.getEnqueuePackageDuration(),
                    () -> sender.accept(pkg)
            );
            distributionMetricsService.getExportedPackageSize().update(pkg.getPkgLength());
            distributionMetricsService.getAcceptedRequests().mark();
            String msg = String.format("Distribution request accepted with type %s paths %s ", request.getRequestType(), Arrays.toString(request.getPaths()));
            log.info(msg);
            return new SimpleDistributionResponse(ACCEPTED, msg);
        } catch (Throwable e) {
            distributionMetricsService.getDroppedRequests().mark();
            String msg = String.format("Failed to queue distribution request %s", e.getMessage());
            log.error(msg, e);
            if (e instanceof Error) {
                throw (Error) e;
            } else {
                throw new DistributionException(msg, e);
            }
        }
    }

    private void sendAndWait(PackageMessage pkg) {
        try {
            CompletableFuture<Void> received = queuedNotifier.registerWait(pkg.getPkgId());
            Event createdEvent = DistributionEvent.eventPackageCreated(pkg, pubAgentName);
            eventAdmin.postEvent(createdEvent);
            sender.accept(pkg);
            received.get(queuedTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            queuedNotifier.unRegisterWait(pkg.getPkgId());
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private DistributionResponse executeUnsupported(DistributionRequest request) {
        String msg = String.format("Request type %s is not supported by this agent, expected one of %s",
                request.getRequestType(), REQ_TYPES.keySet());
        log.info(msg);
        return new SimpleDistributionResponse(DistributionRequestState.DROPPED, msg);
    }
}
