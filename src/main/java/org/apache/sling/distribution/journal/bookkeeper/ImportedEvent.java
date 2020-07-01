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

import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_COMPONENT_KIND;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_COMPONENT_NAME;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_PATHS;
import static org.apache.sling.distribution.event.DistributionEventProperties.DISTRIBUTION_TYPE;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.event.DistributionEventTopics;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.osgi.service.event.Event;

@ParametersAreNonnullByDefault
class ImportedEvent {

    public static final String PACKAGE_ID = "distribution.package.id";
    private static final String KIND_IMPORTER = "importer";
    private PackageMessage pkgMsg;
    private String agentName;

    ImportedEvent(PackageMessage pkgMsg, String agentName) {
        this.pkgMsg = pkgMsg;
        this.agentName = agentName;
    }
    
    Event toEvent() {
        String[] paths = pkgMsg.getPaths().toArray(new String[0]);
        Map<String, Object> props = new HashMap<>();
        props.put(DISTRIBUTION_COMPONENT_KIND, KIND_IMPORTER);
        props.put(DISTRIBUTION_COMPONENT_NAME, agentName);
        props.put(DISTRIBUTION_TYPE, pkgMsg.getReqType().name());
        props.put(DISTRIBUTION_PATHS, paths);
        props.put(PACKAGE_ID, pkgMsg.getPkgId());
        return new Event(DistributionEventTopics.IMPORTER_PACKAGE_IMPORTED, props);
    }

}
