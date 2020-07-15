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

import static org.apache.sling.distribution.journal.HandlerAdapter.create;

import java.io.Closeable;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.queue.CacheCallback;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagingCacheCallback implements CacheCallback {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    private final MessagingProvider messagingProvider;

    private final String packageTopic;

    private final DistributionMetricsService distributionMetricsService;
    
    public MessagingCacheCallback(MessagingProvider messagingProvider, String packageTopic, DistributionMetricsService distributionMetricsService) {
        this.messagingProvider = messagingProvider;
        this.packageTopic = packageTopic;
        this.distributionMetricsService = distributionMetricsService;
    }

    @Override
    public Closeable createConsumer(MessageHandler<PackageMessage> handler) {
        log.info("Starting consumer");
        QueueCacheSeeder seeder = new QueueCacheSeeder(messagingProvider.createSender(packageTopic)); //NOSONAR
        Closeable poller = messagingProvider.createPoller( //NOSONAR
                packageTopic,
                Reset.latest,
                create(PackageMessage.class, (info, message) -> { seeder.close(); handler.handle(info, message); }) 
                ); 
        seeder.startSeeding();
        return () -> IOUtils.closeQuietly(seeder, poller);
    }
    
    @Override
    public List<FullMessage<PackageMessage>> fetchRange(long minOffset, long maxOffset) throws InterruptedException {
        distributionMetricsService.getQueueCacheFetchCount().increment();
        return new RangePoller(messagingProvider, packageTopic, minOffset, maxOffset)
                .fetchRange();
    }

}
