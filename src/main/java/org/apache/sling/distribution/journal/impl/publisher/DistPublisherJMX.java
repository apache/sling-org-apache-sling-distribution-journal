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

import java.util.Set;

import javax.management.MBeanException;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.apache.sling.distribution.queue.spi.DistributionQueue;

class DistPublisherJMX extends StandardMBean implements DistPublisherJMXMBean {

    private static final String COL_ID = "ID";
    private static final String COL_OFFSET = "offset";
    private String pubAgentName;
    private DiscoveryService discoveryService;
    private DistributionPublisher distPublisher;

    protected DistPublisherJMX(String pubAgentName, 
            DiscoveryService discoveryService,
            DistributionPublisher distPublisher
            ) throws NotCompliantMBeanException {
        super(DistPublisherJMXMBean.class);
        this.pubAgentName = pubAgentName;
        this.discoveryService = discoveryService;
        this.distPublisher = distPublisher;
    }

    @Override
    public TabularData getOffsetPerSubscriber() throws MBeanException {
        try {
            String[] itemNames = new String[] {COL_ID, COL_OFFSET};
            OpenType<?>[] itemTypes = new OpenType[]{SimpleType.STRING, SimpleType.LONG};
            CompositeType rowType = new CompositeType("Offsets", "Offsets by sub agent", itemNames, itemNames, itemTypes);
            TabularType type = new TabularType("type", "desc", rowType, new String[] {COL_ID});
            TabularDataSupport table = new TabularDataSupport(type);
            Set<State> subscribedAgents = discoveryService.getTopologyView().getSubscribedAgents(pubAgentName);
            for (State state : subscribedAgents) {
                CompositeData row = new CompositeDataSupport(rowType, itemNames, new Object[] { 
                        state.getSubAgentId(), 
                        state.getOffset()
                        });
                table.put(row);
            }
            return table;
        } catch (OpenDataException e) {
            throw new MBeanException(e);
        }
    }
    
    @Override
    public TabularData getQueue(String queueName) throws MBeanException {
        try {
            String[] itemNames = new String[] {COL_ID, COL_OFFSET};
            OpenType<?>[] itemTypes = new OpenType[]{SimpleType.STRING, SimpleType.LONG};
            CompositeType rowType = new CompositeType("Offsets", "Queue Offsets", itemNames, itemNames, itemTypes);
            TabularType type = new TabularType("type", "desc", rowType, new String[] {COL_ID});
            TabularDataSupport table = new TabularDataSupport(type);
            DistributionQueue queue = distPublisher.getQueue(queueName);
            if (queue != null) {
                for (DistributionQueueEntry item : queue.getEntries(0, 1000)) {
                    CompositeData row = new CompositeDataSupport(rowType, itemNames,
                            new Object[] { item.getId(), item.getItem().get(COL_OFFSET)});
                    table.put(row);
                }
            }
            return table;
        } catch (OpenDataException e) {
            throw new MBeanException(e);
        }
    }
    
}