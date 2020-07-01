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

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import org.apache.sling.distribution.journal.shared.JMXRegistration;
import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.DistributionQueueItemState;
import org.apache.sling.distribution.queue.DistributionQueueItemStatus;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@SuppressWarnings("rawtypes")
public class DistPublisherJMXTest {

    private static final String SUB1_ID = "sling1sub1";
    private static final String TYPE = "PublishAgent";
    private static final String AGENT1 = "agent1";
    private static final String AGENT2 = "agent2";

    @Mock
    DiscoveryService discoveryService;
    
    @Mock
    private DistributionPublisher distPublisher;

    @Mock
    private DistributionQueue queue;

    private DistPublisherJMX bean;

    private JMXRegistration reg;
    
    
    @Before
    public void before() throws NotCompliantMBeanException {
        MockitoAnnotations.initMocks(this);
        Set<State> states = new HashSet<>(Arrays.asList(
                new State(AGENT1, "sling1_sub1", 0, 10, 0, -1, false),
                new State(AGENT1, "sling2_sub1", 0, 15, 0, -1, false),
                new State(AGENT2, "sling2_sub1", 0,  5, 0, -1, false)
                ));
        TopologyView topologyView = new TopologyView(states);
        when(discoveryService.getTopologyView()).thenReturn(topologyView);
        
        DistributionQueueEntry entry1 = createQueueEntry();
        List<DistributionQueueEntry> items = asList(entry1);
        when(queue.getEntries(Mockito.anyInt(), Mockito.anyInt())).thenReturn(items);
        when(distPublisher.getQueue(SUB1_ID + "-error")).thenReturn(queue);
        bean = new DistPublisherJMX(AGENT1, discoveryService, distPublisher);
        reg = new JMXRegistration(bean, TYPE, AGENT1);
    }

    private DistributionQueueEntry createQueueEntry() {
        Map<String, Object> content = new HashMap<>();
        content.put("offset", 10L);
        DistributionQueueItem item = new DistributionQueueItem("packageid", content);
        DistributionQueueItemStatus status = new DistributionQueueItemStatus(DistributionQueueItemState.ERROR, "name");
        return new DistributionQueueEntry("id", item, status);
    }
    
    @After
    public void after() {
        reg.close();
    }
    
    @Test
    public void testCallOffsetPerSubScriber() throws MBeanException {
        TabularData table = bean.getOffsetPerSubscriber();
        checkTable(idToOffset(table));
    }
    
    @Test
    public void testCallQueue() throws MBeanException {
        TabularData table = bean.getQueue(SUB1_ID + "-error");
        checkQueueTable(table);
    }

    @Test
    public void testJMXOffsetPerSubScriber() throws Exception {
        MBeanServer mbeanServer = getPlatformMBeanServer();
        ObjectName name = new ObjectName("org.apache.sling.distribution:type=" + TYPE + ",id=" + AGENT1);
        TabularData table = (TabularData) mbeanServer.getAttribute(name, "OffsetPerSubscriber");
        checkTable(idToOffset(table));
    }

	private void checkTable(Map table) {
        assertTrue(table.containsKey("sling1_sub1"));
        assertThat(table.get("sling1_sub1"), equalTo(10L));
    }

    private void checkQueueTable(TabularData table) {
        CompositeData row1 = (CompositeData)table.values().iterator().next();
        assertThat(row1.get("ID"), equalTo("id"));
        assertThat(row1.get("offset"), equalTo(10L));
    }

    private Map idToOffset(TabularData table) {
        return ((TabularDataSupport) table).entrySet().stream()
                .collect(Collectors.toMap(
                a -> ((CompositeData)a.getValue()).get("ID"),
                b -> ((CompositeData)b.getValue()).get("offset")));
    }

    
}
