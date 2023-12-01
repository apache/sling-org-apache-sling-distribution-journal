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

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.osgi.service.event.EventAdmin;

public class PackageQueuedNotifierTest {

    private PackageQueuedNotifier notifier;

    @Mock
    private EventAdmin eventAdmin;

    @Before
    public void before() throws Exception {
        MockitoAnnotations.openMocks(this).close();
    }

    @Test
    public void test() throws InterruptedException, ExecutionException, TimeoutException {
        notifier = new PackageQueuedNotifier(eventAdmin);
        CompletableFuture<Long> arrived = notifier.registerWait("1");
        notifier.queued(singletonList(fullPkgMsg("2", 0)));
        try {
            arrived.get(100,TimeUnit.MILLISECONDS);
            Assert.fail("Expected TimeoutException");
        } catch (TimeoutException e) {
            // Expected
        }
        notifier.queued(singletonList(fullPkgMsg("1", 1)));
        arrived.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testForNullPackage() throws InterruptedException, ExecutionException, TimeoutException {
        notifier = new PackageQueuedNotifier(eventAdmin);
        CompletableFuture<Long> arrived = notifier.registerWait("1");
        notifier.queued(emptyList());
        try {
            arrived.get(100,TimeUnit.MILLISECONDS);
            Assert.fail("Expected TimeoutException as package ID is null");
        } catch (TimeoutException e) {
            // Expected
        }
    }

    private FullMessage<PackageMessage> fullPkgMsg(String pkgId, long offset) {
        return new FullMessage<>(new TestMessageInfo("topic_package", 0, offset, currentTimeMillis()), pkgMsg(pkgId));
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
