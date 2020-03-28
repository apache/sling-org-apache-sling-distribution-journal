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
package org.apache.sling.distribution.journal.service.subscriber;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.jackrabbit.vault.packaging.Packaging;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventAdmin;

@Component(service = BookKeeperFactory.class, immediate = true)
public class BookKeeperFactory {
    


    @Reference
    private ResourceResolverFactory resolverFactory;
    
    @Reference
    private EventAdmin eventAdmin;
    
    @Reference
    private SubscriberMetrics subscriberMetrics;
    
    @Reference
    private Packaging packaging;

    private BundleContext context;

    private Integer idleMillies;
    
    public BookKeeperFactory() {
    }
    
    public BookKeeperFactory(ResourceResolverFactory resolverFactory,
            EventAdmin eventAdmin, SubscriberMetrics subscriberMetrics, Packaging packaging) {
        this.resolverFactory = resolverFactory;
        this.eventAdmin = eventAdmin;
        this.subscriberMetrics = subscriberMetrics;
        this.packaging = packaging;
    }

    public void activate(BundleContext context, Map<String, Object> properties) {
        this.context = context;
        this.idleMillies = (Integer) properties.getOrDefault("idleMillies", SubscriberIdle.DEFAULT_IDLE_TIME_MILLIS);
        requireNonNull(resolverFactory);
        requireNonNull(eventAdmin);
        requireNonNull(subscriberMetrics);
    }
    
    public BookKeeper create(
            DistributionPackageBuilder packageBuilder,
            String subAgentName, 
            String subSlingId, 
            int maxRetries, 
            boolean editable, 
            PackageHandling packageHandling, 
            Consumer<PackageStatusMessage> sender
            ) {
        ContentPackageExtractor extractor = new ContentPackageExtractor(packaging, packageHandling);
        PackageHandler packageHandler = new PackageHandler(packageBuilder, extractor);
        SubscriberIdle subscriberIdle = new SubscriberIdle(context, this.idleMillies);
        return new BookKeeper(resolverFactory, subscriberMetrics, packageHandler, subscriberIdle, eventAdmin,
                sender, subAgentName, subSlingId, editable, maxRetries);
    }
}
