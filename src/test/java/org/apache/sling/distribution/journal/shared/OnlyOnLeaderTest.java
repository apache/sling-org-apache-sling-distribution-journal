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

import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEvent.Type;
import org.apache.sling.discovery.TopologyView;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import static org.apache.sling.discovery.TopologyEvent.Type.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class OnlyOnLeaderTest {

    @Mock
    private BundleContext context;

    @Mock
    private ServiceRegistration<OnlyOnLeader> reg;

    @InjectMocks
    private OnlyOnLeader onlyOnLeader;

    @Before
    public void before () throws Exception {
        when(context.registerService(eq(OnlyOnLeader.class), any(OnlyOnLeader.class), any()))
                .thenReturn(reg);
    }

    @Test
    public void testServiceRegistration() {
        assertFalse(onlyOnLeader.isLeader());
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_CHANGED, true));
        verify(context).registerService(eq(OnlyOnLeader.class), eq(onlyOnLeader), any());
        assertTrue(onlyOnLeader.isLeader());
    }

    @Test
    public void testDoubleServiceRegistration() {
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_CHANGED, true));
        assertTrue(onlyOnLeader.isLeader());
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_CHANGED, true));
        assertTrue(onlyOnLeader.isLeader());
        verify(context, times(1)).registerService(eq(OnlyOnLeader.class), eq(onlyOnLeader), any());
    }

    @Test
    public void testServiceDeRegistration() {
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_CHANGED, true));
        assertTrue(onlyOnLeader.isLeader());
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_CHANGING, true));
        verify(reg).unregister();
        assertFalse(onlyOnLeader.isLeader());
    }

    @Test
    public void testTopologyChangedOnLeader() {
        assertFalse(onlyOnLeader.isLeader());
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_CHANGED, true));
        assertTrue(onlyOnLeader.isLeader());
    }

    @Test
    public void testTopologyChangingOnLeader() {
        assertFalse(onlyOnLeader.isLeader());
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_CHANGING, true));
        assertFalse(onlyOnLeader.isLeader());
    }

    @Test
    public void testTopologyInitOnLeader() {
        assertFalse(onlyOnLeader.isLeader());
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_INIT, true));
        assertTrue(onlyOnLeader.isLeader());
    }

    @Test
    public void testTopologyChangedOnFollower() {
        assertFalse(onlyOnLeader.isLeader());
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_CHANGED, false));
        assertFalse(onlyOnLeader.isLeader());
    }

    @Test
    public void testTopologyChangingOnFollower() {
        assertFalse(onlyOnLeader.isLeader());
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_CHANGING, false));
        assertFalse(onlyOnLeader.isLeader());
    }

    @Test
    public void testTopologyInitOnFollower() {
        assertFalse(onlyOnLeader.isLeader());
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_INIT, false));
        assertFalse(onlyOnLeader.isLeader());
    }

    @Test
    public void testDeactivate() {
        onlyOnLeader.handleTopologyEvent(topologyEvent(TOPOLOGY_CHANGED, true));
        assertTrue(onlyOnLeader.isLeader());
        onlyOnLeader.deactivate();
        assertFalse(onlyOnLeader.isLeader());
    }

    private TopologyEvent topologyEvent(Type eventType, boolean isLeader) {
        InstanceDescription instanceDescription = mock(InstanceDescription.class);
        when(instanceDescription.isLeader()).thenReturn(isLeader);
        final TopologyView newView;
        if (eventType != TOPOLOGY_CHANGING) {
            newView = mock(TopologyView.class);
            when(newView.getLocalInstance()).thenReturn(instanceDescription);
        } else {
            newView = null;
        }
        final TopologyView oldView;
        if (eventType != TOPOLOGY_INIT) {
            oldView = mock(TopologyView.class);
        } else {
            oldView = null;
        }
        return new TopologyEvent(eventType, oldView, newView);
    }
}