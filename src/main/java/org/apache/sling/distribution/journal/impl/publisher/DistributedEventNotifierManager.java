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

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEventListener;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.bookkeeper.LocalStore;
import org.apache.sling.distribution.journal.impl.discovery.TopologyChangeHandler;
import org.apache.sling.distribution.journal.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.shared.Topics;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import static org.apache.sling.discovery.TopologyEvent.Type;
import static org.apache.sling.discovery.TopologyEvent.Type.TOPOLOGY_CHANGED;
import static org.apache.sling.discovery.TopologyEvent.Type.TOPOLOGY_CHANGING;
import static org.apache.sling.discovery.TopologyEvent.Type.TOPOLOGY_INIT;

@Component(immediate = true, service = TopologyEventListener.class)
@Designate(ocd = DistributedEventNotifierManager.Configuration.class)
public class DistributedEventNotifierManager implements TopologyEventListener {

    /*
     * Register the package distributed event notifier service
     * on all or only the leader instance in a cluster according
     * to the configuration.
     */

    @Reference
    private EventAdmin eventAdmin;

    @Reference
    private PubQueueProvider pubQueueCacheService;

    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    @Reference
    private ResourceResolverFactory resolverFactory;

    private ServiceRegistration<TopologyChangeHandler> reg;

    private BundleContext context;

    private Configuration config;

    private Map<String, LocalStore> localStores;

    private boolean isLeader;

    @Activate
    public void activate(BundleContext context, Configuration config) {
        this.context = context;
        this.config = config;
        this.localStores = new HashMap<>();
        if (! config.deduplicateEvent()) {
            registerService();
        }
    }

    public void deactivate() {
        unregisterService();
    }

    @Override
    public void handleTopologyEvent(TopologyEvent event) {
        if (config.deduplicateEvent()) {
            Type eventType = event.getType();
            if (eventType == TOPOLOGY_INIT || eventType == TOPOLOGY_CHANGED) {
                if (event.getNewView().getLocalInstance().isLeader()) {
                    registerService();
                    this.isLeader = true;
                } else {
                    unregisterService();
                    this.isLeader = false;
                }
            } else if (eventType == TOPOLOGY_CHANGING) {
                unregisterService();
                this.isLeader = false;

            }
        }
    }

    protected boolean isLeader() {
        return this.isLeader;
    }

    private void registerService() {
        if (reg == null) {
            TopologyChangeHandler notifier = new PackageDistributedNotifier(eventAdmin, pubQueueCacheService, messagingProvider, topics, resolverFactory, localStores);
            reg = context.registerService(TopologyChangeHandler.class, notifier, new Hashtable<>());
        }
    }

    private void unregisterService() {
        if (reg != null) {
            reg.unregister();
        }
    }

    @ObjectClassDefinition(name = "Apache Sling Journal based Distribution - Package Distributed Event Notifier Configuration",
            description = "Apache Sling Content Distribution Package Distributed Event Notifier Configuration")
    public @interface Configuration {

        @AttributeDefinition(name = "Deduplicate event",
                description = "When true the distributed event will be sent only on one instance in the cluster. " +
                        "When false the distributed event will be sent on all instances in the cluster. Default is false")
        boolean deduplicateEvent() default false;
    }
}
