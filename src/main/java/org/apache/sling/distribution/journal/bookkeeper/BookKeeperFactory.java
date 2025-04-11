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
package org.apache.sling.distribution.journal.bookkeeper;

import java.util.function.Consumer;

import org.apache.jackrabbit.vault.packaging.Packaging;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.ImportPostProcessor;
import org.apache.sling.distribution.ImportPreProcessor;
import org.apache.sling.distribution.InvalidationProcessor;
import org.apache.sling.distribution.journal.BinaryStore;
import org.apache.sling.distribution.journal.messages.LogMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventAdmin;

@Component(service = BookKeeperFactory.class)
public class BookKeeperFactory {
    private final ResourceResolverFactory resolverFactory;
    private final EventAdmin eventAdmin;
    private final Packaging packaging;
    private final BinaryStore binaryStore;
    private final ImportPreProcessor importPreProcessor;
    private final ImportPostProcessor importPostProcessor;
    private final InvalidationProcessor invalidationProcessor;

    @Activate
    public BookKeeperFactory(
            @Reference ResourceResolverFactory resolverFactory,
            @Reference EventAdmin eventAdmin,
            @Reference Packaging packaging,
            @Reference BinaryStore binaryStore,
            @Reference ImportPreProcessor importPreProcessor,
            @Reference ImportPostProcessor importPostProcessor,
            @Reference InvalidationProcessor invalidationProcessor) {
        this.resolverFactory = resolverFactory;
        this.eventAdmin = eventAdmin;
        this.packaging = packaging;
        this.binaryStore = binaryStore;
        this.importPreProcessor = importPreProcessor;
        this.importPostProcessor = importPostProcessor;
        this.invalidationProcessor = invalidationProcessor;
    }

    public BookKeeper create(
            DistributionPackageBuilder packageBuilder,
            BookKeeperConfig config,
            Consumer<PackageStatusMessage> sender,
            Consumer<LogMessage> logSender,
            SubscriberMetrics subscriberMetrics) {
        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging,
                subscriberMetrics,
                config.getPackageHandling(),
                config.shouldExtractorOverwriteFolderPrimaryTypes());
        PackageHandler packageHandler = new PackageHandler(packageBuilder, extractor, binaryStore);
        return new BookKeeper(
                resolverFactory,
                subscriberMetrics,
                packageHandler,
                eventAdmin,
                sender,
                logSender,
                config,
                importPreProcessor,
                importPostProcessor,
                invalidationProcessor);
    }
}
