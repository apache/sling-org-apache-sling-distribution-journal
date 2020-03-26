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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.apache.sling.distribution.journal.messages.Messages;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;

public class RangePollerTest {

    private static final int MIN_OFFSET = 10;
    private static final int MAX_OFFSET = 20;
    private static final String TOPIC = "topic";
    
    @Mock
    private MessagingProvider clientProvider;
    
    @Captor
    private ArgumentCaptor<HandlerAdapter<Messages.PackageMessage>> handlerCaptor;
    
    @Mock
    private Closeable poller;
    
    private MessageHandler<PackageMessage> handler;
    
    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        when(clientProvider.assignTo(MIN_OFFSET))
                .thenReturn("0:" + MIN_OFFSET);
        when(clientProvider.createPoller(
                Mockito.eq(TOPIC), 
                Mockito.eq(Reset.earliest),
                Mockito.eq("0:" + MIN_OFFSET),
                handlerCaptor.capture()))
        .thenReturn(poller);
    }

    @After
    public void after() throws IOException {
        verify(poller).close();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {
        RangePoller poller = new RangePoller(clientProvider, TOPIC, MIN_OFFSET, MAX_OFFSET);
        handler = handlerCaptor.getValue().getHandler();
        FullMessage<PackageMessage> message1 = simulateMessage(MIN_OFFSET);
        FullMessage<PackageMessage> message2 = simulateMessage(19);
        simulateMessage(MAX_OFFSET);
        List<FullMessage<PackageMessage>> actualMessages = poller.fetchRange();
        assertThat(actualMessages, contains(samePropertyValuesAs(message1), samePropertyValuesAs(message2)));
    }

    private FullMessage<PackageMessage> simulateMessage(int offset) {
        FullMessage<PackageMessage> message = createMessage(offset);
        handler.handle(message.getInfo(), message.getMessage());
        return message;
    }

    private FullMessage<Messages.PackageMessage> createMessage(int offset) {
        MessageInfo info = new TestMessageInfo(TOPIC, 0, offset, System.currentTimeMillis());
        PackageMessage message = Messages.PackageMessage.newBuilder()
                .setPubAgentName("agent1")
                .setPubSlingId("pub1SlingId")
                .setPkgId("package-" + offset)
                .setReqType(ReqType.ADD)
                .setPkgType("journal")
                .addPaths("path")
                .build();
        return new FullMessage<Messages.PackageMessage>(info, message);
    }

}
