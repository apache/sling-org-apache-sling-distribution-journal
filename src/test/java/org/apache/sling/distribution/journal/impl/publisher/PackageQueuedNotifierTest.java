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

import static org.apache.sling.distribution.event.DistributionEventTopics.AGENT_PACKAGE_QUEUED;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.sling.distribution.journal.impl.event.DistributionEvent;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.junit.Assert;
import org.junit.Test;
import org.osgi.service.event.Event;

public class PackageQueuedNotifierTest {

    private static final String PUB_AGENT_ID = "agent1";
    private PackageQueuedNotifier notifier;

    @Test
    public void test() throws InterruptedException, ExecutionException, TimeoutException {
        notifier = new PackageQueuedNotifier();
        CompletableFuture<Void> arrived = notifier.registerWait("1");
        notifier.handleEvent(DistributionEvent.eventPackageQueued(pkgMsg("2"), PUB_AGENT_ID));
        try {
            arrived.get(100,TimeUnit.MILLISECONDS);
            Assert.fail("Expected TimeoutException");
        } catch (TimeoutException e) {
            // Expected
        }
        notifier.handleEvent(DistributionEvent.eventPackageQueued(pkgMsg("1"), PUB_AGENT_ID));
        arrived.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testForNullPackage() throws InterruptedException, ExecutionException, TimeoutException {
        notifier = new PackageQueuedNotifier();
        CompletableFuture<Void> arrived = notifier.registerWait("1");
        notifier.handleEvent(new Event(AGENT_PACKAGE_QUEUED, new HashMap<>()));
        try {
            arrived.get(100,TimeUnit.MILLISECONDS);
            Assert.fail("Expected TimeoutException as package ID is null");
        } catch (TimeoutException e) {
            // Expected
        }
    }

    private PackageMessage pkgMsg(String packageId) {
        return PackageMessage.builder()
            .paths(Arrays.asList("/test"))
            .pkgId(packageId)
            .reqType(ReqType.ADD)
            .pkgType("journal")
            .pubSlingId("sling1")
            .build();
    }
}
