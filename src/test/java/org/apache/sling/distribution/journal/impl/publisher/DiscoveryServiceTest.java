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

import static org.apache.sling.distribution.journal.messages.Messages.SubscriberConfiguration;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

import org.apache.sling.distribution.journal.impl.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.sling.distribution.journal.messages.Messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.Messages.SubscriberState;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;

import org.osgi.framework.BundleContext;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class DiscoveryServiceTest {

    private static final String SUB1_SLING_ID = UUID.randomUUID().toString();
    private static final String SUB1_AGENT = "subagent";
    private static final String PUB1_AGENT_NAME = "pubagent1";
    
    private Closeable poller;
    private MessageHandler<DiscoveryMessage> discoveryHandler;
    private DiscoveryService discoveryService;

    @Before
    public void before() {
        mockDiscoveryService();
    }
    
    @Test
    public void testDiscovery() throws IOException {
        String subAgentId = SUB1_SLING_ID + "-" + SUB1_AGENT; 
        assertTrue(discoveryService.getTopologyView().getSubscriberAgentStates(subAgentId).isEmpty());
        
        MessageInfo info = new TestMessageInfo("topic", 0, 0, 0);
        DiscoveryMessage message = discoveryMessage(SUB1_SLING_ID, SUB1_AGENT, PUB1_AGENT_NAME, 10);
        discoveryHandler.handle(info, message);

        discoveryService.run();
        assertThat(discoveryService.getTopologyView().getState(subAgentId, PUB1_AGENT_NAME).getOffset(), equalTo(10l));
    }
    
    @After
    public void after() throws IOException {
        discoveryService.deactivate();
        verify(poller).close();
    }
    
    @Test
    public void testPurgeNonRespondingSubscriber() {
        // TODO If a subscriber does not respond after a certain timeout its offsets must be purged
    }

    private void mockDiscoveryService() {
        poller = mock(Closeable.class);
        BundleContext bundleContext = mock(BundleContext.class);
        MessagingProvider clientProvider = mock(MessagingProvider.class);
        ArgumentCaptor<HandlerAdapter> captureHandler =  ArgumentCaptor.forClass(HandlerAdapter.class);
        when(clientProvider.createPoller(
                Mockito.anyString(), 
                Mockito.any(Reset.class),
                captureHandler.capture())).thenReturn(poller);
        Topics topics = mock(Topics.class);
        TopologyChangeHandler topologyChangeHandler = mock(TopologyChangeHandler.class);
        discoveryService = new DiscoveryService(clientProvider, topologyChangeHandler, topics);
        discoveryService.activate(bundleContext);
        discoveryHandler = captureHandler.getValue().getHandler();
    }

    private DiscoveryMessage discoveryMessage(String subSlingId, String subAgentName, String pubAgentName, int offset) {
        SubscriberState queueoffset = SubscriberState.newBuilder()
                .setPubAgentName(pubAgentName)
                .setOffset(offset).build();
        
        return DiscoveryMessage.newBuilder()
                .setSubSlingId(subSlingId)
                .setSubAgentName(subAgentName)
                .setSubscriberConfiguration(SubscriberConfiguration
                        .newBuilder()
                        .setEditable(false)
                        .setMaxRetries(-1)
                        .build())
                .addSubscriberState(queueoffset).build();
    }
}
