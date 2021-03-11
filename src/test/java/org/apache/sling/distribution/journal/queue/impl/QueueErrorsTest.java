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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.sling.distribution.journal.messages.LogMessage;
import org.apache.sling.distribution.journal.shared.AgentId;
import org.junit.Before;
import org.junit.Test;
import org.osgi.service.event.Event;

import static org.apache.sling.distribution.journal.impl.discovery.DiscoveryService.TOPIC_DISTRIBUTION_LOG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class QueueErrorsTest {

    private QueueErrors queueErrors;

    @Before
    public void before () {
        queueErrors = new QueueErrors();
    }

    @Test
    public void testEmptyQueueError() {
        assertNull(queueErrors.getError("anyPubAgentName", "anySubAgentId"));
    }

    @Test
    public void testKeepTrackPerPubAgent() {
        String
                pubAgentName = "pubName",
                subAgentName = "subName",
                subSlingId = UUID.randomUUID().toString(),
                subAgentId = new AgentId(subSlingId, subAgentName).getAgentId(),
                message = "Failed to process the item xyz",
                trace = "Failed to process\nRaised here\nfrom here";

        queueErrors.handleEvent(newEvent(pubAgentName, subSlingId, subAgentName, message, trace));

        assertNull(queueErrors.getError(pubAgentName + "another-pub-agent", subAgentId));
        assertNull(queueErrors.getError(pubAgentName, subAgentId + "another-sub-agent"));
        Throwable error = queueErrors.getError(pubAgentName, subAgentId);
        assertNotNull(error);
        assertEquals(message, error.getMessage());
    }

    private Event newEvent(String pubAgentName, String subSlingId, String subAgentName, String message, String trace) {
        return newEvent(new LogMessage(pubAgentName, subSlingId, subAgentName, message, trace));
    }

    private Event newEvent(LogMessage msg) {
        Map<String, Object> props = new HashMap<>();
        props.put("message", msg);
        return new Event(TOPIC_DISTRIBUTION_LOG, props);
    }

}