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

import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyView;
import org.apache.sling.discovery.impl.common.DefaultInstanceDescriptionImpl;
import org.apache.sling.discovery.impl.topology.TopologyViewImpl;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.discovery.TopologyChangeHandler;
import org.apache.sling.distribution.journal.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.EventAdmin;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.osgi.util.converter.Converters.standardConverter;

public class DistributedEventNotifierManagerTest {
    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private PubQueueProvider pubQueueCacheService;

    @Mock
    private MessagingProvider messagingProvider;

    @Spy
    private Topics topics;

    @Spy
    private ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();

    @Mock
    private ServiceRegistration<TopologyChangeHandler> reg;

    @InjectMocks
    private DistributedEventNotifierManager notifierManager;

    private final BundleContext context = MockOsgi.newBundleContext();

    private DistributedEventNotifierManager.Configuration config;

    @Before
    public void before() {
        initMocks(this);
        Map<String, Boolean> config = new HashMap<>();
        config.put("deduplicateEvent", true);
        notifierManager.activate(context, configuration(config, DistributedEventNotifierManager.Configuration.class));
    }

    @Test
    public void testHandleTopologyEvent() {
        TopologyView oldView = new TopologyViewImpl();
        TopologyView newView = newViewWithInstanceDescription(true);

        TopologyEvent event = new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, newView);
        notifierManager.handleTopologyEvent(event);
        assertTrue(notifierManager.isLeader());

        event = new TopologyEvent(TopologyEvent.Type.TOPOLOGY_CHANGED, oldView, newView);
        notifierManager.handleTopologyEvent(event);
        assertTrue(notifierManager.isLeader());

        newView = newViewWithInstanceDescription(false);
        event = new TopologyEvent(TopologyEvent.Type.TOPOLOGY_CHANGED, oldView, newView);
        notifierManager.handleTopologyEvent(event);
        assertFalse(notifierManager.isLeader());

        event = new TopologyEvent(TopologyEvent.Type.TOPOLOGY_CHANGING, oldView, null);
        notifierManager.handleTopologyEvent(event);
        assertFalse(notifierManager.isLeader());
    }

    private TopologyView newViewWithInstanceDescription(boolean isLeader) {
        InstanceDescription description = new DefaultInstanceDescriptionImpl(null, isLeader, true, "slingId", null);
        return new TopologyViewImpl(Arrays.asList(description));
    }

    private <T> T configuration(Map<String, Boolean> props, Class<T> clazz) {
        return standardConverter()
                .convert(props)
                .to(clazz);
    }

}
