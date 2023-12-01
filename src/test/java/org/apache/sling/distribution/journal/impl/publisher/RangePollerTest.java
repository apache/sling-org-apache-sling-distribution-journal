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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;

import java.util.Arrays;
import java.util.List;

import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.msg.InMemoryProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

public class RangePollerTest {

    private static final int MIN_OFFSET = 10;
    private static final int MAX_OFFSET = 20;
    private static final String TOPIC = "topic";
    private static final int SEED_DELAY_SECONDS = 1;
    
    private InMemoryProvider clientProvider;
    
    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageMessage>> handlerCaptor;
    
    @Before
    public void before() throws Exception {
        clientProvider = new InMemoryProvider();
        MockitoAnnotations.openMocks(this).close();
    }

    @Test
    public void testRangeAvailable() throws Exception {
        RangePoller poller = new RangePoller(clientProvider, TOPIC, MIN_OFFSET, MAX_OFFSET, RangePoller.DEFAULT_SEED_DELAY_SECONDS);
        MessageSender<PackageMessage> sender = clientProvider.createSender(TOPIC);
        clientProvider.setLatestOffset(TOPIC, MIN_OFFSET);
        PackageMessage msg1 = createMessage(ReqType.ADD, MIN_OFFSET); // Should be fetched
        PackageMessage msg2 = createMessage(ReqType.TEST, MIN_OFFSET + 1); // Should not be fetched as it is a test message
        PackageMessage msg3 = createMessage(ReqType.ADD, MAX_OFFSET - 1); // Should be fetched
        PackageMessage msg4 = createMessage(ReqType.ADD, MAX_OFFSET); // Should not be fetched as outside range
        
        sender.send(msg1);
        sender.send(msg2);
        clientProvider.setLatestOffset(TOPIC, MAX_OFFSET - 1);
        sender.send(msg3);
        sender.send(msg4);

        List<FullMessage<PackageMessage>> actualMessages = poller.fetchRange();
        
        assertThat(actualMessages.size(), equalTo(2));
        assertThat(actualMessages.get(0).getMessage(), samePropertyValuesAs(msg1));
        assertThat(actualMessages.get(1).getMessage(), samePropertyValuesAs(msg3));
    }
    
    @Test
    public void testRangeNotYetAvailable() throws Exception {
        RangePoller poller = new RangePoller(clientProvider, TOPIC, MIN_OFFSET, MAX_OFFSET, SEED_DELAY_SECONDS);
        
        // Simulate that no message within the range is on the topic but the topic offset already is >= MAX_OFFSET
        // We expect the poller to wait for messages to come in and then send a single seeding message which should let the poller succeed
        clientProvider.setLatestOffset(TOPIC, MAX_OFFSET);
        
        List<FullMessage<PackageMessage>> actualMessages = poller.fetchRange();
        
        assertThat(actualMessages.size(), equalTo(0));
    }
    
    public static PackageMessage createMessage(ReqType reqType, long offset) {
        return PackageMessage.builder()
                .pubAgentName("agent1")
                .pubSlingId("pub1SlingId")
                .pkgId("package-" + offset)
                .reqType(reqType)
                .pkgType("journal")
                .paths(Arrays.asList("path"))
                .build();
    }

}
