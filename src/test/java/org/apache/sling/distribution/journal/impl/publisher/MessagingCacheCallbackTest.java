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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.discovery.DiscoveryService;
import org.apache.sling.distribution.journal.impl.discovery.State;
import org.apache.sling.distribution.journal.impl.discovery.TopologyView;
import org.apache.sling.distribution.journal.messages.ClearCommand;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.queue.QueueState;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
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
    private static final String SUBAGENT_NAME_1 = "subagent1";
    private static final long CLEAR_OFFSET = 7;
    private static final long CURRENT_OFFSET = 1l;
    private static final int HEAD_RETRIES = 2;
    private static final int MAX_RETRIES = 3;

    private static final String PUB1AGENT1 = "agent1";

    private static final String SLINGID1 = UUID.randomUUID().toString();
    private static final String SUBAGENT_ID1 = SLINGID1 +"-" + SUBAGENT_NAME_1;


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
    private DiscoveryService discovery;
    
    @Mock
    private Counter counter;

    @InjectMocks
    private MessagingCacheCallback callback;

    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageMessage>> handlerCaptor;
    
    @Captor
    private ArgumentCaptor<ClearCommand> clearCommandCaptor;

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
    
    @Test
    public void testGetSubscribedAgentIds() {
        TopologyView topology = createTopologyView();
        when(discovery.getTopologyView()).thenReturn(topology);
        Set<String> agentIds = callback.getSubscribedAgentIds(PUB1AGENT1);
        assertThat(agentIds.size(), equalTo(1));
        assertThat(agentIds.iterator().next(), equalTo(SUBAGENT_ID1));
    }
    
    @Test
    public void testGetQueueState() {
        TopologyView topology = createTopologyView();
        when(discovery.getTopologyView()).thenReturn(topology);
        
        QueueState queueState = callback.getQueueState(PUB1AGENT1, SUBAGENT_ID1);
        
        assertThat(queueState.getLastProcessedOffset(), equalTo(CURRENT_OFFSET));
        assertThat(queueState.getHeadRetries(), equalTo(HEAD_RETRIES));
        assertThat(queueState.getMaxRetries(), equalTo(MAX_RETRIES));
        
        queueState.getClearCallback().clear(CLEAR_OFFSET);
        
        verify(sender).accept(clearCommandCaptor.capture());
        ClearCommand clearCommand = clearCommandCaptor.getValue();
        assertThat(clearCommand.getOffset(), equalTo(CLEAR_OFFSET));
        assertThat(clearCommand.getPubAgentName(), equalTo(PUB1AGENT1));
        assertThat(clearCommand.getSubAgentName(), equalTo(SUBAGENT_NAME_1));
        assertThat(clearCommand.getSubSlingId(), equalTo(SLINGID1));
    }

    private TopologyView createTopologyView() {
        State state = new State(PUB1AGENT1, SUBAGENT_ID1, 0, 
                CURRENT_OFFSET, HEAD_RETRIES, MAX_RETRIES, true);
        return new TopologyView(Collections.singleton(state));
    }

    private void simulateMessage(int offset) {
        MessageInfo info = new TestMessageInfo("", 0, offset, System.currentTimeMillis());
        FullMessage<PackageMessage> message = new FullMessage<PackageMessage>(info, RangePollerTest.createMessage(ReqType.ADD, offset));
        handlerCaptor.getValue().getHandler().handle(message.getInfo(), message.getMessage());
    }

    private List<FullMessage<PackageMessage>> fetch() {
        try {
            return callback.fetchRange(10l, 20l);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }

}
