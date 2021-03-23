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
package org.apache.sling.distribution.journal.queue.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.sling.distribution.journal.messages.LogMessage;
import org.apache.sling.distribution.journal.shared.AgentId;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;

import static org.apache.sling.distribution.journal.impl.discovery.DiscoveryService.KEY_MESSAGE;
import static org.apache.sling.distribution.journal.impl.discovery.DiscoveryService.TOPIC_DISTRIBUTION_LOG;
import static org.osgi.service.event.EventConstants.EVENT_TOPIC;

/**
 * Keeps track of the processing errors per sub agent.
 */
@Component(service = { QueueErrors.class, EventHandler.class },
        property = EVENT_TOPIC + "=" + TOPIC_DISTRIBUTION_LOG)
public class QueueErrors implements EventHandler {

    /**
     * pubAgentName -> subAgentId -> Throwable
     */
    private final Map<String, Map<String, Throwable>> errors = new ConcurrentHashMap<>();

    /**
     * Return the error raised during the last processing attempt
     *
     * @return a {@code Throwable} or {@code null} if the last processing attempt did not fail
     */
    public Throwable getError(String pubAgentName, String subAgentId) {
        return errors.computeIfAbsent(pubAgentName, this::newPubAgent)
                .get(subAgentId);
    }

    @Override
    public void handleEvent(Event event) {
        LogMessage msg = (LogMessage) event.getProperty(KEY_MESSAGE);
        if (msg != null) {
            String subAgentId = new AgentId(msg.getSubSlingId(), msg.getSubAgentName()).getAgentId();
            errors.computeIfAbsent(msg.getPubAgentName(), this::newPubAgent)
                    .put(subAgentId, toThrowable(msg));
        }
    }

    private Throwable toThrowable(LogMessage msg) {
        return new Throwable(msg.getMessage(), new Throwable(msg.getStacktrace()));
    }

    private Map<String, Throwable> newPubAgent(String pubAgentName) {
        return new ConcurrentHashMap<>();
    }

}
