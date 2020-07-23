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
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_PATHS;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.discovery.State;
import org.apache.sling.distribution.journal.impl.discovery.TopologyView;
import org.apache.sling.distribution.journal.impl.discovery.TopologyViewDiff;
import org.apache.sling.distribution.journal.messages.PackageDistributedMessage;
import org.apache.sling.distribution.journal.queue.OffsetQueue;
import org.apache.sling.distribution.journal.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.queue.QueueItemFactory;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.osgi.service.event.EventAdmin;

public class PackageDistributedNotifierTest {

    @Mock
    private PubQueueProvider pubQueueCacheService;

    @Mock
    private OffsetQueue<DistributionQueueItem> offsetQueue;

    @Spy
    private Topics topics;

    @Mock
    private MessagingProvider messagingProvider;

    @Mock
    private EventAdmin eventAdmin;

    @InjectMocks
    private PackageDistributedNotifier notifier;

    @Mock
    private MessageSender<Object> sender;

    @Captor
    private ArgumentCaptor<PackageDistributedMessage> messageCaptor;

    @Before
    public void before() {
        initMocks(this);
        when(offsetQueue.getItem(anyLong()))
                .thenReturn(createItem());
        when(pubQueueCacheService.getOffsetQueue(anyString(), anyLong()))
                .thenReturn(offsetQueue);
        when(messagingProvider.createSender(Mockito.eq(topics.getEventTopic())))
            .thenReturn(sender);
    }

    @Test
    public void testChanged() throws Exception {
        notifier.activate();
        TopologyViewDiff diffView = new TopologyViewDiff(
                buildView(new State("pub1", "sub1", 1000, 10, 0, -1, false)),
                buildView(new State("pub1", "sub1", 2000, 13, 0, -1, false)));
        notifier.changed(diffView);
        verify(sender, times(3)).accept(messageCaptor.capture());
    }

    private DistributionQueueItem createItem() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(QueueItemFactory.RECORD_OFFSET, 10l);
        properties.put(PROPERTY_REQUEST_PATHS, new String[] {"/test"});
        properties.put(PROPERTY_REQUEST_DEEP_PATHS, new String[] {"/test"});
        return new DistributionQueueItem("packageId", properties);
    }

    private TopologyView buildView(State ... state) {
        return new TopologyView(new HashSet<>(asList(state)));
    }
}