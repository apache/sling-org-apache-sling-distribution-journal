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
package org.apache.sling.distribution.journal.impl.queue;

import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_PACKAGE_TYPE;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_PATHS;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_TYPE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.queue.DistributionQueueItem;

import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;

@ParametersAreNonnullByDefault
public final class QueueItemFactory {

    public static final String RECORD_TOPIC = "recordTopic";

    public static final String RECORD_PARTITION = "recordPartition";

    public static final String RECORD_OFFSET = "recordOffset";

    public static final String RECORD_TIMESTAMP = "recordTimestamp";

    public static final String PACKAGE_MSG = "packageMessage";

    private static final String REQUEST_USER_ID = "internal.request.user";
    
    private QueueItemFactory() {
    }

    public static DistributionQueueItem fromPackage(FullMessage<PackageMessage> fMessage) {
        return fromPackage(fMessage.getInfo(), fMessage.getMessage(), false);
    }
    
    public static DistributionQueueItem fromPackage(
            MessageInfo info, 
            PackageMessage message,
            boolean addMessage) {

        String packageId = message.getPkgId();
        long pkgLength = message.getPkgLength();
        Map<String, Object> properties = new HashMap<>(10);
        properties.put(RECORD_TOPIC, info.getTopic());
        properties.put(RECORD_PARTITION, info.getPartition());
        properties.put(RECORD_OFFSET, info.getOffset());
        properties.put(RECORD_TIMESTAMP, info.getCreateTime());
        properties.put(PROPERTY_PACKAGE_TYPE, message.getPkgType());
        properties.put(PROPERTY_REQUEST_TYPE, toDistReqType(message.getReqType()));
        String[] paths = toArray(message.getPaths());
        properties.put(PROPERTY_REQUEST_PATHS, paths);
        String[] deepPaths = toArray(message.getDeepPaths());
        properties.put(PROPERTY_REQUEST_DEEP_PATHS, deepPaths);
        if (addMessage) {
            properties.put(PACKAGE_MSG, message);
        }
        if (message.getUserId() != null) {
            properties.put(REQUEST_USER_ID, message.getUserId());
        }
        return new DistributionQueueItem(packageId, pkgLength, properties);
    }
    
    private static DistributionRequestType toDistReqType(ReqType reqType) {
        switch (reqType) {
        case ADD:
            return DistributionRequestType.ADD;
        case DELETE:
            return DistributionRequestType.DELETE;
        case TEST:
            return DistributionRequestType.TEST;
        default:
            throw new IllegalArgumentException("Unhandled DistributionRequestType: " + reqType.name());
        }
    }

	@Nonnull
    private static String[] toArray(List<String> paths) {
        return paths.isEmpty() ? new String[]{} : paths.toArray(new String[0]);
    }
}
