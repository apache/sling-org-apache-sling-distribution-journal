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

import java.util.Hashtable;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.discovery.DiscoveryService;
import org.apache.sling.distribution.journal.messages.ClearCommand;
import org.apache.sling.distribution.journal.queue.CacheCallback;
import org.apache.sling.distribution.journal.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.queue.PubQueueProviderFactory;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.Topics;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

/**
 * Normally PubQueueProvider should be created per publish agent.
 * For compatibility with current code and to save on number of consumers 
 * we must make sure to publish only one for the messaging based impl.
 */
@Component
public class PubQueueProviderPublisher {
    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private DiscoveryService discoveryService;

    @Reference
    private Topics topics;
    
    @Reference
    private DistributionMetricsService distributionMetricsService;

    @Reference
    private PubQueueProviderFactory pubQueueProviderFactory;

    private PubQueueProvider pubQueueProvider;

    private ServiceRegistration<PubQueueProvider> reg;

    @Activate
    public void activate(BundleContext context) {
        Consumer<ClearCommand> commandSender = messagingProvider.createSender(topics.getCommandTopic());
        CacheCallback callback = new MessagingCacheCallback(
                messagingProvider, 
                topics.getPackageTopic(), 
                distributionMetricsService,
                discoveryService,
                commandSender);
        this.pubQueueProvider = pubQueueProviderFactory.create(callback);
        reg = context.registerService(PubQueueProvider.class, this.pubQueueProvider, new Hashtable<>());
    }
    
    @Deactivate
    public void deactivate() {
        IOUtils.closeQuietly(this.pubQueueProvider);
        reg.unregister();
    }
}
