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

import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEventListener;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;

import static org.apache.sling.discovery.TopologyEvent.Type.*;

/**
 * Marker service to register services to enable components
 * only on the leader within a cluster. To leverage it,
 * add a reference to <code>OnlyOnLeader</code> in your
 * services that must only run on the leader within a cluster.
 *
 */
@Component(immediate = true, service = TopologyEventListener.class)
public class OnlyOnLeader implements TopologyEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(OnlyOnLeader.class);

    private final BundleContext context;

    private volatile ServiceRegistration<OnlyOnLeader> reg;

    @Activate
    public OnlyOnLeader(BundleContext context) {
        this.context = context;
    }

    @Deactivate
    public void deactivate() {
        unregister();
    }

    @Override
    public void handleTopologyEvent(TopologyEvent event) {
        TopologyEvent.Type eventType = event.getType();
        if (eventType == TOPOLOGY_INIT || eventType == TOPOLOGY_CHANGED) {
            if (event.getNewView().getLocalInstance().isLeader()) {
                register();
            } else {
                unregister();
            }
        } else if (eventType == TOPOLOGY_CHANGING) {
            unregister();
        }
    }

    boolean isLeader() {
        return reg != null;
    }

    private synchronized void register() {
        if (reg == null) {
            LOG.info("Registering OnlyOnLeader service");
            reg = context.registerService(OnlyOnLeader.class, this, new Hashtable<>());
        } else {
            LOG.debug("Service OnlyOnLeader already registered");
        }
    }

    private synchronized void unregister() {
        if (reg != null) {
            LOG.info("Unregistering OnlyOnLeader service");
            reg.unregister();
            reg = null;
        } else {
            LOG.debug("No OnlyOnLeader service to unregister");
        }
    }
}
