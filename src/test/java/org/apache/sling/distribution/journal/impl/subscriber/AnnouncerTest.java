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

import org.apache.sling.distribution.journal.messages.Messages;
import org.apache.sling.distribution.journal.MessageSender;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.sling.distribution.journal.impl.queue.impl.PackageRetries;

public class AnnouncerTest {

    private static final String SUB1_SLING_ID = "sub1sling";
    private static final String PUB1_AGENT_NAME = "pub1agent";
    private static final String SUB1_AGENT_NAME = "sub1agent";

    @Test
    @SuppressWarnings("unchecked")
    public void testDiscoveryMessage() throws InterruptedException {
        MessageSender<Messages.DiscoveryMessage> sender = Mockito.mock(MessageSender.class);
        LocalStore offsetStore = Mockito.mock(LocalStore.class);
        PackageRetries packageRetries = Mockito.mock(PackageRetries.class);
        when(offsetStore.load("offset", -1L)).thenReturn(1l);
        Announcer announcer = new Announcer(SUB1_SLING_ID, SUB1_AGENT_NAME, "discoverytopic", Collections.singleton(PUB1_AGENT_NAME), sender, offsetStore, packageRetries, -1, false, 10000);
        Thread.sleep(200);
        ArgumentCaptor<Messages.DiscoveryMessage> msg = forClass(Messages.DiscoveryMessage.class);
        verify(sender).send(Mockito.eq("discoverytopic"), msg.capture());
        Messages.DiscoveryMessage message = msg.getValue();
        Messages.SubscriberState offset = message.getSubscriberStateList().iterator().next();
        assertThat(message.getSubSlingId(), equalTo(SUB1_SLING_ID));
        assertThat(offset.getPubAgentName(), equalTo(PUB1_AGENT_NAME));
        assertThat(offset.getOffset(), equalTo(1l));
        announcer.close();
    }
}
