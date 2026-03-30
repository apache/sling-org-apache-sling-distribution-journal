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
package org.apache.sling.distribution.journal.queue;

import java.io.Closeable;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.impl.publisher.PackageQueuedNotifier;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.spi.DistributionQueue;

@ParametersAreNonnullByDefault
public interface PubQueueProvider extends Closeable {

    /**
     * Virtual queue name for aggregate mode: backlog from the minimum {@code lastProcessedOffset}
     * among clearable subscribers; clear applies to every clearable subscriber.
     */
    String AGGREGATED_QUEUE_PERSISTED = "persisted";

    /**
     * Virtual queue name for aggregate mode: backlog from the minimum {@code lastProcessedOffset}
     * among all subscribers with queue state; not clearable.
     */
    String AGGREGATED_QUEUE_PUBLIC = "public";

    @Nullable
    DistributionQueue getQueue(String pubAgentName, String queueName);
    
    /**
     * Maximum backlog depth for a subscriber cohort (min {@code lastProcessedOffset} in cohort, then journal tail size).
     *
     * @param pubAgentName name of the pub agent
     * @param clearable {@code true} for clearable subscribers (non-null clear callback); {@code false} for non-clearable
     *                  (queue state present, null clear callback). Publisher throttling uses {@code clearable == true}.
     * @return max size for that cohort or 0 if there are none
     */
    int getMaxQueueSize(String pubAgentName, boolean clearable);

    @Nonnull
    OffsetQueue<DistributionQueueItem> getOffsetQueue(String pubAgentName, long minOffset);

    void handleStatus(MessageInfo info, PackageStatusMessage message);

    /**
     * Get queue names for alive subscribed subscriber agents.
     */
    Set<String> getQueueNames(String pubAgentName);

    /**
     * Queue names when {@code aggregateSubscriberQueues} is enabled on the publisher:
     * {@link #AGGREGATED_QUEUE_PERSISTED} (omitted if there is no clearable subscriber)
     * and {@link #AGGREGATED_QUEUE_PUBLIC} (omitted if no subscriber has queue state).
     */
    @Nonnull
    Set<String> getAggregatedQueueNames(String pubAgentName);

    /**
     * Resolve a virtual queue from {@link #getAggregatedQueueNames(String)}.
     *
     * @return {@code null} if {@code queueName} is not an aggregated queue or the cohort is empty
     */
    @Nullable
    DistributionQueue getAggregatedQueue(String pubAgentName, String queueName);

    @Nonnull
    PackageQueuedNotifier getQueuedNotifier();

}
