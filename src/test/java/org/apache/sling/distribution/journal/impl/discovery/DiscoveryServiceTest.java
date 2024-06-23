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
package org.apache.sling.distribution.journal.impl.discovery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.LogMessage;
import org.apache.sling.distribution.journal.messages.SubscriberConfig;
import org.apache.sling.distribution.journal.messages.SubscriberState;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;

@RunWith(MockitoJUnitRunner.class)
public class DiscoveryServiceTest {

    private static final String SUB1_SLING_ID = UUID.randomUUID().toString();
    private static final String SUB1_AGENT = "subagent";
    private static final String PUB1_AGENT_NAME = "pubagent1";
    
    @Mock
    private Closeable poller;
    
    @Mock
    BundleContext bundleContext;

    @Mock
    MessagingProvider clientProvider;
    
    @Captor
    ArgumentCaptor<HandlerAdapter<DiscoveryMessage>> captureDiscoveryHandler;
    
    @Captor
    ArgumentCaptor<HandlerAdapter<LogMessage>> captureLogHandler;
    
    @Captor
    ArgumentCaptor<Event> captureEvent;
    
    @Mock
    TopologyChangeHandler topologyChangeHandler;

    @Mock
    private EventAdmin eventAdmin;

    private MessageHandler<DiscoveryMessage> discoveryHandler;
    private MessageHandler<LogMessage> logHandler;
    
    private DiscoveryService discoveryService;
    
    @Before
    public void before() {
        discoveryService = new DiscoveryService(clientProvider, topologyChangeHandler, eventAdmin);
        when(clientProvider.createPoller(
                Mockito.anyString(), 
                Mockito.any(Reset.class),
                captureDiscoveryHandler.capture(), captureLogHandler.capture())).thenReturn(poller);
        discoveryService.activate(bundleContext);
        discoveryHandler = captureDiscoveryHandler.getValue().getHandler();
        logHandler = captureLogHandler.getValue().getHandler();
    }
    
    @Test
    public void testDiscoveryEventChangesTopologyView() throws IOException {
        String subAgentId = SUB1_SLING_ID + "-" + SUB1_AGENT; 
        assertTrue(discoveryService.getTopologyView().getSubscriberAgentStates(subAgentId).isEmpty());
        
        DiscoveryMessage message = discoveryMessage(SUB1_SLING_ID, SUB1_AGENT,
                subscriberState(PUB1_AGENT_NAME, 10));
        discoveryHandler.handle(messageInfo(0), message);

        discoveryService.run();
        assertThat(discoveryService.getTopologyView().getState(subAgentId, PUB1_AGENT_NAME).getOffset(), equalTo(10L));
    }
    
    @Test
    public void testReceivingLogMessageSendsOSGiEvent() {
        LogMessage logMessage = LogMessage.builder().build();
        logHandler.handle(messageInfo(0), logMessage);
        verify(eventAdmin).postEvent(captureEvent.capture());
        Event event = captureEvent.getValue();
        assertThat(event.getTopic(), equalTo(DiscoveryService.TOPIC_DISTRIBUTION_LOG));
        assertThat(event.getProperty(DiscoveryService.KEY_MESSAGE), equalTo(logMessage));
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

    private MessageInfo messageInfo(int offset) {
        return new TestMessageInfo("topic", 0, offset, 0);
    }

    private DiscoveryMessage discoveryMessage(String subSlingId, String subAgentName, SubscriberState... subStates) {
        return DiscoveryMessage.builder()
                .subSlingId(subSlingId)
                .subAgentName(subAgentName)
                .subscriberConfiguration(SubscriberConfig
                        .builder()
                        .editable(false)
                        .maxRetries(-1)
                        .build())
                .subscriberStates(Arrays.asList(subStates)).build();
    }

    private SubscriberState subscriberState(String pubAgentName, int offset) {
        return SubscriberState.builder()
                .pubAgentName(pubAgentName)
                .offset(offset).build();
    }
}
