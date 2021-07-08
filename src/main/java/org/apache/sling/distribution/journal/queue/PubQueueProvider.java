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

    @Nullable
    DistributionQueue getQueue(String pubAgentName, String queueName);

    @Nonnull
    OffsetQueue<DistributionQueueItem> getOffsetQueue(String pubAgentName, long minOffset);

    void handleStatus(MessageInfo info, PackageStatusMessage message);

    /**
     * Get queue names for alive subscribed subscriber agents.
     */
    Set<String> getQueueNames(String pubAgentName);

    @Nonnull
    PackageQueuedNotifier getQueuedNotifier();

}
