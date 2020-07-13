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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.Topics;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MessagingCacheCallbackTest {
    @Mock
    private MessagingProvider messagingProvider;
    
    @Spy
    private Topics topics;
    
    @Mock
    private JournalAvailable journalAvailable;
    
    @Mock
    private DistributionMetricsService distributionMetricsService;
    
    @Mock
    private MessageHandler<PackageMessage> handler;

    @Mock
    private MessageSender<Object> sender;
    
    @Mock
    private Counter counter;

    @InjectMocks
    private MessagingCacheCallback callback;

    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageMessage>> handlerCaptor;

    @Test
    public void testCreateConsumer() throws Exception {
        when(messagingProvider.createSender(Mockito.any())).thenReturn(sender);
        Closeable poller = callback.createConsumer(handler);
        assertThat(poller, notNullValue());
        
        poller.close();
    }

    @Test
    public void testFetchRange() throws Exception {
        when(distributionMetricsService.getQueueCacheFetchCount()).thenReturn(counter);
        when(messagingProvider.assignTo(Mockito.eq(10l))).thenReturn("0:10");
        CompletableFuture<List<FullMessage<PackageMessage>>> result = CompletableFuture.supplyAsync(this::fetch);
        verify(messagingProvider, timeout(100000)).createPoller(
                Mockito.anyString(), 
                Mockito.eq(Reset.earliest), 
                Mockito.eq("0:10"),
                handlerCaptor.capture());
        simulateMessage(19);
        simulateMessage(20);
        List<FullMessage<PackageMessage>> messages = result.get(100, TimeUnit.SECONDS);
        assertThat(messages.size(), equalTo(1));
    }

    private void simulateMessage(int offset) {
        FullMessage<PackageMessage> message = RangePollerTest.createMessage(ReqType.ADD, offset);
        handlerCaptor.getValue().getHandler().handle(message.getInfo(), message.getMessage());
    }

    List<FullMessage<PackageMessage>> fetch() {
        try {
            return callback.fetchRange(10l, 20l);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }

}
