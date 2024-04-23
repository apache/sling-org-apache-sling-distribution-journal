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

import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.distribution.journal.*;
import org.apache.sling.distribution.journal.impl.discovery.DiscoveryService;
import org.apache.sling.distribution.journal.impl.discovery.State;
import org.apache.sling.distribution.journal.impl.discovery.TopologyView;
import org.apache.sling.distribution.journal.impl.discovery.TopologyViewDiff;
import org.apache.sling.distribution.journal.messages.PackageDistributedMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.queue.CacheCallback;
import org.apache.sling.distribution.journal.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.queue.impl.PubQueueProviderImpl;
import org.apache.sling.distribution.journal.queue.impl.QueueErrors;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.*;
import org.osgi.framework.BundleContext;
import org.osgi.service.event.EventAdmin;

import java.io.Closeable;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;

@Ignore
public class PackageDistributedNotifierTest {

    private static final String PUB_AGENT_NAME = "publish1";
    private static final String SUB_AGENT_NAME = "subscriber1";

    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private PubQueueProvider pubQueueCacheService;

    @Mock
    private MessagingProvider messagingProvider;

    @Spy
    private Topics topics;

    @Spy
    private ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();

    @Mock
    private MessageSender<Object> sender;

    @Captor
    private ArgumentCaptor<PackageDistributedMessage> messageCaptor;

    @Captor
    private ArgumentCaptor<MessageHandler<PackageMessage>> handlerCaptor;

    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageStatusMessage>> statHandlerCaptor;

    @Mock
    private CacheCallback callback;

    @Mock
    private Closeable poller;

    @Mock
    private Closeable statPoller;

    private final BundleContext context = MockOsgi.newBundleContext();

    private MessageHandler<PackageMessage> handler;

    private PubQueueProviderImpl queueProvider;

    private PackageDistributedNotifier notifier;
    private MetricsService metricsService = MetricsService.NOOP;
    
    @Mock
    private DiscoveryService discoveryService;
    
    @Before
    public void before() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        when(callback.createConsumer(handlerCaptor.capture()))
                .thenReturn(poller);
        when(messagingProvider.createPoller(
                Mockito.eq(Topics.STATUS_TOPIC),
                any(Reset.class),
                statHandlerCaptor.capture()))
                .thenReturn(statPoller);
        URI serverURI = new URI("http://myserver.apache.org:1234/somepath");
        when(messagingProvider.getServerUri()).thenReturn(serverURI);
        when(messagingProvider.createSender(Mockito.eq(topics.getEventTopic())))
            .thenReturn(sender);

        QueueErrors queueErrors = mock(QueueErrors.class);
        queueProvider = new PubQueueProviderImpl(eventAdmin,
                queueErrors,
                discoveryService,
                topics,
                metricsService,
                messagingProvider,
                context);
        handler = handlerCaptor.getValue();
        for(int i = 0; i <= 20; i++)
            handler.handle(info(i), packageMessage("packageid" + i, PUB_AGENT_NAME));

        notifier = new PackageDistributedNotifier(eventAdmin, pubQueueCacheService, messagingProvider, topics, resolverFactory, true);
    }

    @Test
    public void testChanged() throws Exception {
        TopologyViewDiff diffView = new TopologyViewDiff(
                buildView(new State(PUB_AGENT_NAME, SUB_AGENT_NAME, 1000, 10, 0, -1, false)),
                buildView(new State(PUB_AGENT_NAME, SUB_AGENT_NAME, 2000, 13, 0, -1, false)));
        when(pubQueueCacheService.getOffsetQueue(PUB_AGENT_NAME, 11))
                .thenReturn(queueProvider.getOffsetQueue(PUB_AGENT_NAME, 11));
        notifier.changed(diffView);
        verify(sender, times(3)).accept(messageCaptor.capture());
    }

    @Test
    public void testPersistLastRaisedOffset() throws Exception {
        TopologyViewDiff diffView1 = new TopologyViewDiff(
                buildView(new State(PUB_AGENT_NAME, SUB_AGENT_NAME, 1000, 10, 0, -1, false)),
                buildView(new State(PUB_AGENT_NAME, SUB_AGENT_NAME, 2000, 13, 0, -1, false)));
        // there is no value for the last raised offset persisted in the author repository
        when(pubQueueCacheService.getOffsetQueue(PUB_AGENT_NAME, 11))
                .thenReturn(queueProvider.getOffsetQueue(PUB_AGENT_NAME, 11));
        notifier.changed(diffView1);
        verify(sender, times(3)).accept(messageCaptor.capture());

        notifier.storeLastDistributedOffset();

        TopologyViewDiff diffView2 = new TopologyViewDiff(
                buildView(new State(PUB_AGENT_NAME, SUB_AGENT_NAME, 1000, 15, 0, -1, false)),
                buildView(new State(PUB_AGENT_NAME, SUB_AGENT_NAME, 2000, 20, 0, -1, false)));
        // the last raised offset persisted in the author repository is 13
        when(pubQueueCacheService.getOffsetQueue(PUB_AGENT_NAME, 13))
                .thenReturn(queueProvider.getOffsetQueue(PUB_AGENT_NAME, 13));
        notifier.changed(diffView2);
        verify(sender, times(3 + 5)).accept(messageCaptor.capture());

        notifier.storeLastDistributedOffset();

        notifier = new PackageDistributedNotifier(eventAdmin, pubQueueCacheService, messagingProvider, topics, resolverFactory, false);
        // the last raised offset persisted in the author repository is not considered because `ensureEvent` is disabled
        when(pubQueueCacheService.getOffsetQueue(PUB_AGENT_NAME, 16))
                .thenReturn(queueProvider.getOffsetQueue(PUB_AGENT_NAME, 16));
        notifier.changed(diffView2);
        verify(sender, times(3 + 5 + 5)).accept(messageCaptor.capture());
    }

    private TopologyView buildView(State ... state) {
        return new TopologyView(new HashSet<>(asList(state)));
    }

    private MessageInfo info(long offset) {
        MessageInfo info = Mockito.mock(MessageInfo.class);
        when(info.getOffset()).thenReturn(offset);
        return info;
    }

    private PackageMessage packageMessage(String packageId, String pubAgentName) {
        return PackageMessage.builder()
                .pubAgentName(pubAgentName)
                .pubSlingId("pub1SlingId")
                .pkgId(packageId)
                .reqType(PackageMessage.ReqType.ADD)
                .pkgType("journal")
                .paths(Collections.singletonList("path"))
                .deepPaths(Collections.singletonList("deep-path"))
                .build();
    }

}
