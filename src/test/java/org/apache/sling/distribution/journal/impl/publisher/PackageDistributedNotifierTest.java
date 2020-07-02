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

import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.internal.util.reflection.Whitebox.setInternalState;

import java.util.Collections;
import java.util.HashSet;

import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.journal.impl.queue.impl.PubQueueCacheService;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.osgi.service.event.EventAdmin;

public class PackageDistributedNotifierTest {

    @Mock
    private PubQueueCacheService pubQueueCacheService;

    @Mock
    private OffsetQueue<DistributionQueueItem> offsetQueue;

    @Mock
    private Topics topics;

    @Mock
    private MessagingProvider messagingProvider;

    @Mock
    private EventAdmin eventAdmin;

    @Before
    public void before() {
        initMocks(this);
        when(offsetQueue.getItem(anyLong()))
                .thenReturn(new DistributionQueueItem("packageId", Collections.emptyMap()));
        when(pubQueueCacheService.getOffsetQueue(anyString(), anyLong()))
                .thenReturn(offsetQueue);
    }


    @Test
    public void testChanged() throws Exception {
        PackageDistributedNotifier notifier = Mockito.spy(new PackageDistributedNotifier());
        setInternalState(notifier, "pubQueueCacheService", pubQueueCacheService);
        setInternalState(notifier, "eventAdmin", eventAdmin);
        setInternalState(notifier, "messagingProvider", messagingProvider);
        setInternalState(notifier, "topics", topics);
        notifier.activate();
        TopologyViewDiff diffView = new TopologyViewDiff(
                buildView(new State("pub1", "sub1", 1000, 10, 0, -1, false)),
                buildView(new State("pub1", "sub1", 2000, 13, 0, -1, false)));
        notifier.changed(diffView);
        verify(notifier, times(3)).processOffset(anyString(), any(DistributionQueueItem.class));
    }

    private TopologyView buildView(State ... state) {
        return new TopologyView(new HashSet<>(asList(state)));
    }
}