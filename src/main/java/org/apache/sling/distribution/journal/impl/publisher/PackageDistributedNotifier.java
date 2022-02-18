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


import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.impl.discovery.TopologyChangeHandler;
import org.apache.sling.distribution.journal.impl.discovery.TopologyViewDiff;
import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.sling.distribution.journal.messages.PackageDistributedMessage;
import org.apache.sling.distribution.journal.queue.OffsetQueue;
import org.apache.sling.distribution.journal.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.queue.QueueItemFactory;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.journal.MessagingProvider;

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_PATHS;

@ParametersAreNonnullByDefault
public class PackageDistributedNotifier implements TopologyChangeHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PackageDistributedNotifier.class);

    private final EventAdmin eventAdmin;

    private final PubQueueProvider pubQueueCacheService;

    private Consumer<PackageDistributedMessage> sender;

    private final boolean sendMsg;

    public PackageDistributedNotifier(EventAdmin eventAdmin, PubQueueProvider pubQueueProvider, MessagingProvider messagingProvider, Topics topics) {
        this.eventAdmin = eventAdmin;
        this.pubQueueCacheService = pubQueueProvider;
        sendMsg = StringUtils.isNotBlank(topics.getEventTopic());
        if (sendMsg) {
            sender = messagingProvider.createSender(topics.getEventTopic());
        }
        // TODO load the last processed offset from the store
        //      and set the lastDistributedOffset field with the value
        LOG.info("Started package distributed notifier with event message topic {}", topics.getEventTopic());
    }

    @Override
    public void changed(TopologyViewDiff diffView) {
        diffView.getProcessedOffsets().forEach(this::processOffsets);
    }

    /**
     * @param  pubAgentName the name of the publisher agent
     * @param offsets range of offsets, from smallest offset to largest offset.
     */
    private void processOffsets(String pubAgentName, Supplier<LongStream> offsets) {
        long minOffset = offsets.get().findFirst().getAsLong();
        OffsetQueue<DistributionQueueItem> offsetQueue = pubQueueCacheService.getOffsetQueue(pubAgentName, minOffset);
        offsets
            .get()
            .mapToObj(offsetQueue::getItem)
            .filter(Objects::nonNull)
            .forEach(msg -> notifyDistributed(pubAgentName, msg));
    }

    protected void storeLastDistributedOffset() {
        // TODO load the last processed offset from the store
        //      and compare with lastDistributedOffset in memory
        //      store the new value if it has changed.
    }

    protected void notifyDistributed(String pubAgentName, DistributionQueueItem queueItem) {
        LOG.debug("Sending distributed notifications for pubAgentName={}, pkgId={}", pubAgentName, queueItem.getPackageId());
        sendEvt(pubAgentName, queueItem);
        if (sendMsg) {
            sendMsg(pubAgentName, queueItem);
        }
    }

    private void sendMsg(String pubAgentName, DistributionQueueItem queueItem) {
        try {
            PackageDistributedMessage msg = createDistributedMessage(pubAgentName, queueItem);
            sender.accept(msg);
        } catch (Exception e) {
            LOG.warn("Exception when sending package distributed message for pubAgentName={}, pkgId={}", pubAgentName, queueItem.getPackageId(), e);
        }
    }

    private PackageDistributedMessage createDistributedMessage(String pubAgentName, DistributionQueueItem queueItem) {
        return PackageDistributedMessage.builder()
            .pubAgentName(pubAgentName)
            .packageId(queueItem.getPackageId())
            .offset((Long) queueItem.get(QueueItemFactory.RECORD_OFFSET))
            .paths((String[]) queueItem.get(PROPERTY_REQUEST_PATHS))
            .deepPaths((String[]) queueItem.get(PROPERTY_REQUEST_DEEP_PATHS))
            .build();
    }

    private void sendEvt(String pubAgentName, DistributionQueueItem queueItem) {
        try {
            Event distributed = DistributionEvent.eventPackageDistributed(queueItem, pubAgentName);
            eventAdmin.sendEvent(distributed);
            // TODO update lastProcessedOffset field in memory
        } catch (Exception e) {
            LOG.warn("Exception when sending package distributed event for pubAgentName={}, pkgId={}", pubAgentName, queueItem.getPackageId(), e);
        }
    }
}
