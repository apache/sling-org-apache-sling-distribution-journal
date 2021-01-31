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
package org.apache.sling.distribution.journal.shared;

import java.io.Closeable;
import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.sling.distribution.event.DistributionEventProperties;
import org.apache.sling.distribution.event.DistributionEventTopics;
import org.apache.sling.distribution.journal.impl.discovery.DiscoveryService;
import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.sling.distribution.journal.messages.LogMessage;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;

/**
 * Listens to log message events (for errors) and package distributed events (for success) and logs both to the
 * given DefaultDistributionLog
 */
public class DistributionLogEventListener implements EventHandler, Closeable {
    private static final String[] TOPICS = new String [] {DiscoveryService.TOPIC_DISTRIBUTION_LOG, DistributionEventTopics.AGENT_PACKAGE_DISTRIBUTED};
    private final String pubAgentName;
    private final ServiceRegistration<EventHandler> reg;
    private final DefaultDistributionLog log;
    
    private static final String MESSAGE_AND_STACKTRACE = "Message: {},\nStacktrace: {}";
    
    public DistributionLogEventListener(BundleContext context, DefaultDistributionLog log, String pubAgentName) {
        this.log = log;
        this.pubAgentName = pubAgentName;
        Dictionary<String, Object> eventHandlerProps = new Hashtable<>();
        eventHandlerProps.put(EventConstants.EVENT_TOPIC, TOPICS);
        reg = context.registerService(EventHandler.class, this, eventHandlerProps);
    }

    public void handleEvent(Event event) {
        if (DistributionEventTopics.AGENT_PACKAGE_DISTRIBUTED.equals(event.getTopic())) {
            handleDistributedEvent(event);
        } else if (DiscoveryService.TOPIC_DISTRIBUTION_LOG.equals(event.getTopic())) {
            handleLogEvent(event);
        }
    }

    private void handleLogEvent(Event event) {
        LogMessage logMessage = (LogMessage) event.getProperty(DiscoveryService.KEY_MESSAGE);
        if (!pubAgentName.equals(logMessage.getPubAgentName())) {
            return;
        }
        if (logMessage.getStacktrace() == null) {
            log.info(logMessage.getMessage());
        } else {
            log.warn(MESSAGE_AND_STACKTRACE, logMessage.getMessage(), logMessage.getStacktrace());
        }
    }

    private void handleDistributedEvent(Event event) {
        String agentName = (String)event.getProperty(DistributionEventProperties.DISTRIBUTION_COMPONENT_NAME);
        if (!pubAgentName.equals(agentName)) {
            return;
        }
        String[] paths = (String[])event.getProperty(DistributionEventProperties.DISTRIBUTION_PATHS);
        String type = (String)event.getProperty(DistributionEventProperties.DISTRIBUTION_TYPE);
        String packageId = (String)event.getProperty(DistributionEvent.PACKAGE_ID);
        log.info("Succesfully applied package with id {}, type {}, paths {}", packageId, type, paths);
    }

    @Override
    public void close() {
        if (reg!= null) {
            reg.unregister();
        }
    }
}
