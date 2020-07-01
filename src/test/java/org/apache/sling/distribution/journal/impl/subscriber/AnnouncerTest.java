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
package org.apache.sling.distribution.journal.impl.subscriber;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.function.Consumer;

import org.apache.sling.distribution.journal.bookkeeper.BookKeeper;
import org.apache.sling.distribution.journal.messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.SubscriberState;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class AnnouncerTest {

    private static final String SUB1_SLING_ID = "sub1sling";
    private static final String PUB1_AGENT_NAME = "pub1agent";
    private static final String SUB1_AGENT_NAME = "sub1agent";

    @Test
    @SuppressWarnings("unchecked")
    public void testDiscoveryMessage() throws InterruptedException {
        Consumer<DiscoveryMessage> sender = Mockito.mock(Consumer.class);
        BookKeeper bookKeeper = Mockito.mock(BookKeeper.class);
        when(bookKeeper.loadOffset()).thenReturn(1L);
        Announcer announcer = new Announcer(SUB1_SLING_ID, SUB1_AGENT_NAME, Collections.singleton(PUB1_AGENT_NAME), sender, bookKeeper, -1, false, 10000);
        Thread.sleep(200);
        ArgumentCaptor<DiscoveryMessage> msg = forClass(DiscoveryMessage.class);
        verify(sender).accept(msg.capture());
        DiscoveryMessage message = msg.getValue();
        SubscriberState offset = message.getSubscriberStates().iterator().next();
        assertThat(message.getSubSlingId(), equalTo(SUB1_SLING_ID));
        assertThat(offset.getPubAgentName(), equalTo(PUB1_AGENT_NAME));
        assertThat(offset.getOffset(), equalTo(1L));
        announcer.close();
    }
}
