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
package org.apache.sling.distribution.journal.impl.precondition;


import static org.apache.sling.distribution.journal.HandlerAdapter.create;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.shared.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageStatusWatcher implements Closeable {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final Closeable poller;
    
    // subAgentName -> pkgOffset -> Status
    private final Map<String, NavigableMap<Long, Status>> pkgStatusPerSubAgent = new ConcurrentHashMap<>();


    public PackageStatusWatcher(MessagingProvider messagingProvider, Topics topics) {
        String topicName = topics.getStatusTopic();

        poller = messagingProvider.createPoller(
                topicName,
                Reset.earliest,
                create(PackageStatusMessage.class, this::handle)
        );
    }

    /**
     * Gets the status that confirms the package at offset pkgOffset
     * @param pkgOffset the offset of the package
     * @return the status confirming the package; or null if it has not been confirmed yet
     */
    public PackageStatusMessage.Status getStatus(String subAgentName, long pkgOffset) {
        Map<Long, Status> statusPerAgent = getAgentStatus(subAgentName);
        Status status = statusPerAgent.get(pkgOffset);
        if (status == null && higherStatusAlreadyArrived(subAgentName, pkgOffset)) {
            log.info("Considering offset={} imported as status for this package can not arrive anymore.", pkgOffset);
            return Status.IMPORTED;
        }
        return status;
    }

    private boolean higherStatusAlreadyArrived(String subAgentName, long pkgOffset) {
        NavigableMap<Long, Status> pkgStatus = pkgStatusPerSubAgent.get(subAgentName);
        return pkgStatus.higherKey(pkgOffset) != null;
    }

    private Map<Long, Status> getAgentStatus(String subAgentName) {
        return pkgStatusPerSubAgent.computeIfAbsent(subAgentName, this::newMap);
    }
    
    private NavigableMap<Long, Status> newMap(String subAgentName) {
        return new TreeMap<>();
    }

    @Override
    public void close() throws IOException {
        poller.close();
    }

    private void handle(MessageInfo info, PackageStatusMessage pkgStatusMsg) {
        long statusOffset = pkgStatusMsg.getOffset();
        // TODO: check revision
        Map<Long, Status> agentStatus = getAgentStatus(pkgStatusMsg.getSubAgentName());
        agentStatus.put(statusOffset, pkgStatusMsg.getStatus());
    }
}
