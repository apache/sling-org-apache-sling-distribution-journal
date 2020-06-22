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
package org.apache.sling.distribution.journal.impl.event;

import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_COMPONENT_KIND;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_COMPONENT_NAME;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_PATHS;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_TYPE;
import static org.apache.sling.distribution.event.DistributionEventTopics.AGENT_PACKAGE_CREATED;
import static org.apache.sling.distribution.event.DistributionEventTopics.AGENT_PACKAGE_DISTRIBUTED;
import static org.apache.sling.distribution.event.DistributionEventTopics.AGENT_PACKAGE_QUEUED;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_PATHS;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_TYPE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.osgi.service.event.Event;

@ParametersAreNonnullByDefault
public class DistributionEvent {

    public static final String PACKAGE_ID = "distribution.package.id";
    private static final String KIND_AGENT = "agent";

    private DistributionEvent() {
    }
    
    public static Event eventPackageCreated(PackageMessage pkgMsg, String agentName) {
        return buildEvent(AGENT_PACKAGE_CREATED, KIND_AGENT, agentName, pkgMsg);
    }

    public static Event eventPackageDistributed(DistributionQueueItem queueItem, String agentName) {
        return buildEvent(AGENT_PACKAGE_DISTRIBUTED, KIND_AGENT, agentName,
                queueItem.get(PROPERTY_REQUEST_TYPE, String.class),
                queueItem.get(PROPERTY_REQUEST_PATHS, String[].class),
                queueItem.getPackageId());
    }

    public static Event eventPackageQueued(PackageMessage pkgMsg, String agentName) {
        return buildEvent(AGENT_PACKAGE_QUEUED, KIND_AGENT, agentName, pkgMsg);
    }

    private static Event buildEvent(String topic, String kind, String agentName, PackageMessage pkgMsg) {
        List<String> pathsList = pkgMsg.getPaths();
        return buildEvent(topic, kind, agentName,
                pkgMsg.getReqType().name(),
                pathsList.toArray(new String[0]),
                pkgMsg.getPkgId());
    }

    private static Event buildEvent(String topic, String kind, String agentName, String reqType, String[] paths, String packageId) {
        Map<String, Object> props = new HashMap<>();
        props.put(DISTRIBUTION_COMPONENT_KIND, kind);
        props.put(DISTRIBUTION_COMPONENT_NAME, agentName);
        props.put(DISTRIBUTION_TYPE, reqType);
        props.put(DISTRIBUTION_PATHS, paths);
        props.put(PACKAGE_ID, packageId);
        return new Event(topic, props);

    }

}
