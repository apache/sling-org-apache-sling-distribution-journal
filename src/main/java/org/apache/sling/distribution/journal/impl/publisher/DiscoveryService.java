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
import static org.apache.sling.distribution.journal.HandlerAdapter.create;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;

import java.io.Closeable;
import java.util.Dictionary;
import java.util.Hashtable;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.impl.queue.impl.PubQueueCacheService;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.Messages;
import org.apache.sling.distribution.journal.messages.Messages.SubscriberConfiguration;
import org.apache.commons.io.IOUtils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

import org.apache.sling.distribution.journal.messages.Messages.DiscoveryMessage;
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
 * This component uses lazy starting so it is only started when there is at least one Agent
 * that requires it.
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
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    @Reference
    private TopologyChangeHandler topologyChangeHandler;
    
    @Reference
    private PubQueueCacheService pubQueueCacheService;

    private volatile ServiceRegistration<?> reg;

    private final TopologyViewManager viewManager = new TopologyViewManager(REFRESH_TTL_MS);

    private Closeable poller;

    public DiscoveryService() {
    }

    public DiscoveryService(
            MessagingProvider messagingProvider,
            TopologyChangeHandler topologyChangeHandler,
            Topics topics,
            PubQueueCacheService pubQueueCacheService) {
        this.messagingProvider = messagingProvider;
        this.topologyChangeHandler = topologyChangeHandler;
        this.topics = topics;
        this.pubQueueCacheService = pubQueueCacheService;
    }

    @Activate
    public void activate(BundleContext context) {
        poller = messagingProvider.createPoller(
                topics.getDiscoveryTopic(), 
                Reset.latest,
                create(Messages.DiscoveryMessage.class, new DiscoveryMessageHandler()));
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
            long minOffset = Long.MAX_VALUE;
            for (Messages.SubscriberState subStateMsg : disMsg.getSubscriberStateList()) {
                SubscriberConfiguration subConfig = disMsg.getSubscriberConfiguration();
                State subState = new State(subStateMsg.getPubAgentName(), subAgentId.getAgentId(), now, subStateMsg.getOffset(), subStateMsg.getRetries(), subConfig.getMaxRetries(), subConfig.getEditable());
                viewManager.refreshState(subState);
                minOffset = Math.min(minOffset, subState.getOffset());
            }
            pubQueueCacheService.seed(minOffset);
        }
    }
}
