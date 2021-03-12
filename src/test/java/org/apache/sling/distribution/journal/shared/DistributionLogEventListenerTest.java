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

import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_COMPONENT_NAME;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_PATHS;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_TYPE;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.sling.distribution.event.DistributionEventTopics;
import org.apache.sling.distribution.journal.impl.discovery.DiscoveryService;
import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.sling.distribution.journal.messages.LogMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;

@RunWith(MockitoJUnitRunner.class)
public class DistributionLogEventListenerTest {

    private static final String ERROR_MESSAGE = "Error importing package";

    @Mock
    private BundleContext context;
    
    @Mock
    private ServiceRegistration<EventHandler> reg;

    private DefaultDistributionLog log = new DefaultDistributionLog("agent1", this.getClass(), DefaultDistributionLog.LogLevel.INFO);
    
    private DistributionLogEventListener listener;

    @Before
    public void before() {
        when(context.registerService(Mockito.eq(EventHandler.class), Mockito.any(EventHandler.class), Mockito.any()))
            .thenReturn(reg);
        listener = new DistributionLogEventListener(context, log, "agent1");
    }
    
    @After
    public void after() {
        listener.close();
        verify(reg).unregister();
    }

    @Test
    public void testPackageDistributedEvent() {
        Map<String, Object> props = new HashMap<>();
        props.put(DISTRIBUTION_COMPONENT_NAME, "agent1");
        props.put(DISTRIBUTION_TYPE, "Add");
        props.put(DISTRIBUTION_PATHS, new String[] {"/test"});
        props.put(DistributionEvent.PACKAGE_ID, "packageId");
        Event event = new Event(DistributionEventTopics.AGENT_PACKAGE_DISTRIBUTED, props);
        listener.handleEvent(event);
        String line = log.getLines().iterator().next();
        assertThat(line, endsWith("Successfully applied package with id packageId, type Add, paths [/test]"));
    }
    
    @Test
    public void testLogEvent() {
        LogMessage logMessage = LogMessage.builder()
            .pubAgentName("agent1")
            .message(ERROR_MESSAGE)
            .build();
        Map<String, Object> props = new HashMap<>();
        props.put(DiscoveryService.KEY_MESSAGE, logMessage);
        Event event = new Event(DiscoveryService.TOPIC_DISTRIBUTION_LOG, props);
        listener.handleEvent(event);
        String line = log.getLines().iterator().next();
        assertThat(line, endsWith(ERROR_MESSAGE));
    }

}
