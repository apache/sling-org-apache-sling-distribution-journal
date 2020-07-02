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
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;
import static org.apache.sling.distribution.journal.HandlerAdapter.create;

import java.io.Closeable;
import java.util.Dictionary;
import java.util.Hashtable;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.SubscriberConfig;
import org.apache.sling.distribution.journal.messages.SubscriberState;
import org.apache.sling.distribution.journal.shared.PublisherConfigurationAvailable;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.commons.io.IOUtils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.Reset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens for discovery messages and tracks presence of Subscribers as well as
 * the last processed offset of each Subscriber
 *
 * This component is only activated when there is at least one DistributionSubscriber agent configured.
 *
 * This component is meant to be shared by Publisher agents.
 */
@Component(service = DiscoveryService.class)
@ParametersAreNonnullByDefault
public class DiscoveryService implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryService.class);

    private static final long REFRESH_TTL_MS = 30000;

    @Reference
    private JournalAvailable journalAvailable;

    @Reference
    private PublisherConfigurationAvailable publisherConfigurationAvailable;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    @Reference
    private TopologyChangeHandler topologyChangeHandler;

    private volatile ServiceRegistration<?> reg;

    private final TopologyViewManager viewManager = new TopologyViewManager(REFRESH_TTL_MS);

    private Closeable poller;

    public DiscoveryService() {
    }

    public DiscoveryService(
            MessagingProvider messagingProvider,
            TopologyChangeHandler topologyChangeHandler,
            Topics topics) {
        this.messagingProvider = messagingProvider;
        this.topologyChangeHandler = topologyChangeHandler;
        this.topics = topics;
    }

    @Activate
    public void activate(BundleContext context) {
        poller = messagingProvider.createPoller(
                topics.getDiscoveryTopic(), 
                Reset.latest,
                create(DiscoveryMessage.class, new DiscoveryMessageHandler())
                ); 
        startTopologyViewUpdaterTask(context);
        LOG.info("Discovery service started");
    }

    @Deactivate
    public void deactivate() {
        if (reg != null) {
            reg.unregister();
        }
        IOUtils.closeQuietly(poller);
        LOG.info("Discovery service stopped");
    }

    public TopologyView getTopologyView() {
        return viewManager.getCurrentView();
    }

    @Override
    public void run() {
        TopologyView oldView = viewManager.updateView();
        TopologyView newView = viewManager.getCurrentView();
        handleChanges(newView, oldView);
    }

    private void handleChanges(TopologyView newView, TopologyView oldView) {
        if (! newView.equals(oldView)) {
            String msg = format("TopologyView changed from %s to %s", oldView, newView);
            TopologyViewDiff diffView = new TopologyViewDiff(oldView, newView);
            if (diffView.subscribedAgentsChanged()) {
                LOG.info(msg);
            } else {
                LOG.debug(msg);
            }
            topologyChangeHandler.changed(diffView);
        }
    }

    private void startTopologyViewUpdaterTask(BundleContext context) {
        // Register periodic task to update the topology view
        Dictionary<String, Object> props = new Hashtable<>();
        props.put(PROPERTY_SCHEDULER_CONCURRENT, false);
        props.put(PROPERTY_SCHEDULER_PERIOD, 5L); // every 5 seconds
        reg = context.registerService(Runnable.class.getName(), this, props);
    }

    private final class DiscoveryMessageHandler implements MessageHandler<DiscoveryMessage> {

        @Override
        public void handle(MessageInfo info, DiscoveryMessage disMsg) {

            long now = System.currentTimeMillis();
            AgentId subAgentId = new AgentId(disMsg.getSubSlingId(), disMsg.getSubAgentName());
            for (SubscriberState subStateMsg : disMsg.getSubscriberStates()) {
                SubscriberConfig subConfig = disMsg.getSubscriberConfiguration();
                State subState = new State(subStateMsg.getPubAgentName(), subAgentId.getAgentId(), now, subStateMsg.getOffset(), subStateMsg.getRetries(), subConfig.getMaxRetries(), subConfig.isEditable());
                viewManager.refreshState(subState);
            }
        }
    }
}
