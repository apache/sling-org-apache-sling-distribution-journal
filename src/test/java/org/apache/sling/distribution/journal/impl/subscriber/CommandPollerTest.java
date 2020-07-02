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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;

import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.messages.ClearCommand;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.shared.Topics;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class CommandPollerTest {

    private static final String SUB_AGENT_NAME = "subAgentName";

    private static final String SUB_SLING_ID = "subSlingId";

    private static final String SUBSLING_ID_OTHER = "subslingIdOther";

    private static final String SUB_AGENT_OTHER = "subAgentOther";

    @Mock
    private Closeable poller;
    
    @Mock
    MessagingProvider clientProvider;
    
    CommandPoller commandPoller;
    
    @Captor
    private ArgumentCaptor<HandlerAdapter<ClearCommand>> handlerCaptor;
    
    private MessageHandler<ClearCommand> commandHandler;

    private Topics topics = new Topics();

    private MessageInfo info;

    @Before
    public void before() {
        Awaitility.setDefaultPollDelay(Duration.ZERO);
        Awaitility.setDefaultPollInterval(Duration.ONE_HUNDRED_MILLISECONDS);
        MockitoAnnotations.initMocks(this);
        info = new TestMessageInfo("topic", 0, 0, 0);
    }

    @Test
    public void testSkipped() throws DistributionException, InterruptedException, IOException {
        createCommandPoller();
        
        commandHandler.handle(info, commandMessage(SUBSLING_ID_OTHER, SUB_AGENT_OTHER, 1L));
        assertSkipped();

        commandHandler.handle(info, commandMessage(SUBSLING_ID_OTHER, SUB_AGENT_NAME, 1L));
        assertSkipped();
        
        commandHandler.handle(info, commandMessage(SUB_SLING_ID, SUB_AGENT_OTHER, 1L));
        assertSkipped();
        
        commandPoller.close();
        
        verify(poller).close();
    }
    
    @Test
    public void testClearOffsets() throws DistributionException, InterruptedException, IOException {
        createCommandPoller();

        commandHandler.handle(info, commandMessage(10L));
        assertClearedUpTo(10);
        
        commandHandler.handle(info, commandMessage(11L));
        assertClearedUpTo(11);

        // Clearing lower offset should not change cleared offset
        commandHandler.handle(info, commandMessage(1L));
        assertClearedUpTo(11);
        
        commandPoller.close();
        
        verify(poller).close();
    }

    private void assertClearedUpTo(int max) {
        for (int c=0; c<=max; c++) { 
            assertThat(commandPoller.isCleared(c), equalTo(true));
        }
        assertThat(commandPoller.isCleared(max+1), equalTo(false));

    }

    private void assertSkipped() {
        assertThat(commandPoller.isCleared(1), equalTo(false));
    }

    private ClearCommand commandMessage(long offset) {
        return commandMessage(SUB_SLING_ID, SUB_AGENT_NAME, offset);
    }
    
    private ClearCommand commandMessage(String subSlingId, String subAgentName, long offset) {
        return ClearCommand.builder()
                .subAgentName(subAgentName)
                .subSlingId(subSlingId)
                .offset(offset)
                .build();
    }

    private void createCommandPoller() {
        when(clientProvider.createPoller(
                Mockito.anyString(),
                Mockito.eq(Reset.earliest), 
                handlerCaptor.capture()))
            .thenReturn(poller);
        commandPoller = new CommandPoller(clientProvider, topics, SUB_SLING_ID, SUB_AGENT_NAME);
        commandHandler = handlerCaptor.getValue().getHandler();
    }

}
