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
package org.apache.sling.distribution.journal.queue.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.discovery.DiscoveryService;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.queue.CacheCallback;
import org.apache.sling.distribution.journal.queue.QueueState;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.apache.sling.distribution.queue.spi.DistributionQueue;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.osgi.framework.BundleContext;
import org.osgi.service.event.EventAdmin;

@Ignore
public class PubQueueProviderTest {
    private static final String PUB1_AGENT_NAME = "pub1";
    private static final String PUB2_AGENT_NAME = "pub2";

    private static final String SUB_SLING_ID = UUID.randomUUID().toString();
    private static final String SUB_AGENT_NAME = "sub1";
    private static final String SUB_AGENT_ID = SUB_SLING_ID +"-" + SUB_AGENT_NAME;


    @Mock
    private MessagingProvider clientProvider;
    
    @Captor
    private ArgumentCaptor<MessageHandler<PackageMessage>> handlerCaptor;

    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageStatusMessage>> statHandlerCaptor;

    @Mock
    private Closeable poller;
    
    @Mock
    private Closeable statPoller;

    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private MessageSender<Object> sender;

    @Mock
    private CacheCallback callback;

    private final BundleContext context = MockOsgi.newBundleContext();

    private MessageHandler<PackageMessage> handler;

    private PubQueueProviderImpl queueProvider;
    private MBeanServer mbeanServer;
    private DiscoveryService discoveryService;
    private Topics topics;
    private MetricsService metricsService;
    private MessagingProvider messagingProvider;
    
    @Before
    public void before() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        when(callback.createConsumer(handlerCaptor.capture()))
                .thenReturn(poller);
        when(clientProvider.createPoller(
                Mockito.eq(Topics.STATUS_TOPIC), 
                Mockito.any(Reset.class),
                statHandlerCaptor.capture()))
        .thenReturn(statPoller);
        QueueErrors queueErrors = mock(QueueErrors.class);
        queueProvider = new PubQueueProviderImpl(eventAdmin,
                queueErrors,
                discoveryService,
                topics,
                metricsService,
                messagingProvider,
                context);
        handler = handlerCaptor.getValue();
    }

    @After
    public void after() throws IOException {
        queueProvider.close();
        verify(poller,  atLeast(1)).close();
    }
    
    @SuppressWarnings("null")
    @Test
    public void test() throws Exception {
        handler.handle(info(1L), packageMessage("packageid1", PUB1_AGENT_NAME));
        handler.handle(info(2L), packageMessage("packageid2", PUB2_AGENT_NAME));
        handler.handle(info(3L), packageMessage("packageid3", PUB1_AGENT_NAME));
        
        when(callback.getQueueState(Mockito.eq(PUB1_AGENT_NAME), Mockito.any()))
            .thenReturn(new QueueState(0, -1, 0, null));
        
        // Full pub1 queue contains all packages from pub1
        DistributionQueue queue = queueProvider.getQueue(PUB1_AGENT_NAME, SUB_AGENT_ID);
        Iterator<DistributionQueueEntry> it1 = queue.getEntries(0, -1).iterator();
        assertThat(it1.next().getItem().getPackageId(), equalTo("packageid1"));
        assertThat(it1.next().getItem().getPackageId(), equalTo("packageid3"));
        
        // With offset 1 first package is removed
        when(callback.getQueueState(Mockito.eq(PUB1_AGENT_NAME), Mockito.any()))
            .thenReturn(new QueueState(1, -1, 0, null));
        
        DistributionQueue queue2 = queueProvider.getQueue(PUB1_AGENT_NAME, SUB_AGENT_ID);
        Iterator<DistributionQueueEntry> it2 = queue2.getEntries(0, 20).iterator();
        assertThat(it2.next().getItem().getPackageId(), equalTo("packageid3"));
        assertThat(it2.hasNext(), equalTo(false));
        
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        Set<ObjectInstance> mbeans = mbeanServer.queryMBeans(new ObjectName("org.apache.sling.distribution:type=OffsetQueue,id="+PUB1_AGENT_NAME), null);
        ObjectInstance mbean = mbeans.iterator().next();
        assertThat(getAttrib(mbean, "Size"), equalTo(2));
        assertThat(getAttrib(mbean, "HeadOffset"), equalTo(1L));
        assertThat(getAttrib(mbean, "TailOffset"), equalTo(3L));
    }
    
    @Test
    public void testQueueSizeWithTwoPackages() throws Exception {
        handler.handle(info(0L), packageMessage("packageid1", PUB1_AGENT_NAME));
        handler.handle(info(1L), packageMessage("packageid2", PUB2_AGENT_NAME));
        handler.handle(info(2L), packageMessage("packageid3", PUB1_AGENT_NAME));
        when(callback.getSubscribedAgentIds(PUB1_AGENT_NAME)).thenReturn(Collections.singleton("sub1"));
        when(callback.getQueueState(Mockito.eq(PUB1_AGENT_NAME), Mockito.any()))
            .thenReturn(new QueueState(0, -1, 0, null));

        int size = queueProvider.getMaxQueueSize(PUB1_AGENT_NAME);
        assertThat(size, equalTo(2));
    }
    
    @Test
    public void testQueueSizeWithoutPackages() throws Exception {
        handler.handle(info(1L), packageMessage("packageid2", PUB2_AGENT_NAME));
        when(callback.getSubscribedAgentIds(PUB1_AGENT_NAME)).thenReturn(Collections.singleton("sub1"));
        when(callback.getQueueState(Mockito.eq(PUB1_AGENT_NAME), Mockito.any()))
            .thenReturn(new QueueState(0, -1, 0, null));

        int size = queueProvider.getMaxQueueSize(PUB1_AGENT_NAME);
        assertThat(size, equalTo(0));
    }
    
    @SuppressWarnings("null")
    @Test
    public void testEmptyErrorQueue() throws Exception {
        DistributionQueue queue = queueProvider.getQueue(PUB1_AGENT_NAME, SUB_AGENT_ID + "-error");
        assertThat(queue.getStatus().getItemsCount(), equalTo(0));
    }
    
    @SuppressWarnings("null")
    @Test
    public void testErrorQueue() throws Exception {
        // TODO Test empty error queue when stat but no package for it

        // Simulate receive of package message and status message
        PackageMessage pkgMsg1 = packageMessage("packageid1", PUB1_AGENT_NAME);
        MessageInfo info = info(1L);
        handler.handle(info, pkgMsg1);
        PackageStatusMessage statusMsg1 = statusMessage(info.getOffset(), pkgMsg1);
        queueProvider.handleStatus(info, statusMsg1);
        
        DistributionQueue queue = queueProvider.getQueue(PUB1_AGENT_NAME, SUB_AGENT_ID + "-error");
        assertThat(queue.getStatus().getItemsCount(), equalTo(1));
        DistributionQueueEntry head = queue.getHead();
        DistributionQueueItem item = head.getItem();
        assertThat(item.getPackageId(), equalTo("packageid1")); 
    }

    @Test
    public void testMultipleCloseInvocations() {
        queueProvider.close();
        queueProvider.close();
    }
    
    @Test
    public void testCleanUp() {
        handler.handle(info(0L), packageMessage("packageid1", PUB1_AGENT_NAME));

        assertThat(queueSize(), equalTo(1));
        queueProvider.run();
        assertThat(queueSize(), equalTo(1));
        
        for (long c=0; c<10001;c++) {
            handler.handle(info(c), packageMessage("packageid" + c, PUB1_AGENT_NAME));
        }
        assertThat(queueSize(), equalTo(10001));
        queueProvider.run();
        handler = handlerCaptor.getValue();
        handler.handle(info(0L), packageMessage("packageid1", PUB1_AGENT_NAME));
        assertThat(queueSize(), equalTo(1));
    }

    private int queueSize() {
        return queueProvider.getOffsetQueue(PUB1_AGENT_NAME, 0).getSize();
    }

    private MessageInfo info(long offset) {
        MessageInfo info = Mockito.mock(MessageInfo.class);
        when(info.getOffset()).thenReturn(offset);
        return info;
    }

    private PackageStatusMessage statusMessage(long offset, PackageMessage pkgMsg1) {
        return PackageStatusMessage.builder()
            .offset(offset)
            .pubAgentName(PUB1_AGENT_NAME)
            .status(Status.REMOVED_FAILED)
            .subAgentName(SUB_AGENT_NAME)
            .subSlingId(SUB_SLING_ID)
            .build();
    }

    private Object getAttrib(ObjectInstance mbean, String key)
            throws InstanceNotFoundException, ReflectionException, AttributeNotFoundException, MBeanException {
        return mbeanServer.getAttribute(mbean.getObjectName(), key);
    }

    private PackageMessage packageMessage(String packageId, String pubAgentName) {
        return PackageMessage.builder()
                .pubAgentName(pubAgentName)
                .pubSlingId("pub1SlingId")
                .pkgId(packageId)
                .reqType(ReqType.ADD)
                .pkgType("journal")
                .paths(List.of("path"))
                .deepPaths(List.of("deep-path"))
                .build();
    }
}
