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
package org.apache.sling.distribution.journal.impl.queue.impl;

import static java.lang.System.currentTimeMillis;
import static org.apache.sling.distribution.journal.shared.Topics.PACKAGE_TOPIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.LongConsumer;

import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueueCacheSeederTest {

    @Mock
    private MessagingProvider clientProvider;

    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageMessage>> pkgHandlerCaptor;

    @Captor
    private ArgumentCaptor<PackageMessage> pkgMsgCaptor;

    @Mock
    private Closeable poller;

    @Mock
    private MessageSender<PackageMessage> sender;

    @Mock
    private LongConsumer callback;

    private QueueCacheSeeder seeder;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        when(clientProvider.createPoller(
                eq(PACKAGE_TOPIC),
                any(Reset.class),
                pkgHandlerCaptor.capture()))
                .thenReturn(poller);
        doNothing().when(sender).send(pkgMsgCaptor.capture());
        when(clientProvider.<PackageMessage>createSender(eq(PACKAGE_TOPIC)))
                .thenReturn(sender);
        seeder = new QueueCacheSeeder(clientProvider, PACKAGE_TOPIC);
    }

    @Test
    public void testSeededCallback() throws IOException {
        seeder.seed(callback);
        long offset = 15L;
        simulateSeedingMsg(offset);
        verify(callback).accept(offset);
        verify(poller).close();
    }

    @Test
    public void testSendingSeeds() {
        seeder.seed(callback);
        verify(sender, timeout(5000).atLeastOnce()).send(pkgMsgCaptor.capture());
        PackageMessage seedMsg = pkgMsgCaptor.getValue();
        assertNotNull(seedMsg);
        assertEquals(ReqType.TEST, seedMsg.getReqType());
    }

    @After
    public void after() {
        seeder.close();
    }

    private void simulateSeedingMsg(long offset) {
        PackageMessage msg = seeder.createTestMessage();
        pkgHandlerCaptor.getValue().getHandler().handle(
                new TestMessageInfo(PACKAGE_TOPIC, 0, offset, currentTimeMillis()),
                msg);
    }
}