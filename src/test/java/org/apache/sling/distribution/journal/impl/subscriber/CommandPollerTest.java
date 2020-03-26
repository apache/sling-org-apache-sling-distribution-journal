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
import static org.mockito.Mockito.never;
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
import org.apache.sling.distribution.journal.messages.Messages.ClearCommand;
import org.apache.sling.distribution.journal.messages.Messages.CommandMessage;
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
    private ArgumentCaptor<HandlerAdapter<CommandMessage>> handlerCaptor;
    
    private MessageHandler<CommandMessage> commandHandler;

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
        createCommandPoller(true);
        
        commandHandler.handle(info, commandMessage(SUBSLING_ID_OTHER, SUB_AGENT_OTHER, 1l));
        assertSkipped();

        commandHandler.handle(info, commandMessage(SUBSLING_ID_OTHER, SUB_AGENT_NAME, 1l));
        assertSkipped();
        
        commandHandler.handle(info, commandMessage(SUB_SLING_ID, SUB_AGENT_OTHER, 1l));
        assertSkipped();
    }
    
    @Test
    public void testClearOffsets() throws DistributionException, InterruptedException, IOException {
        createCommandPoller(true);

        commandHandler.handle(info, commandMessage(10l));
        assertClearedUpTo(10);
        
        commandHandler.handle(info, commandMessage(11l));
        assertClearedUpTo(11);

        // Clearing lower offset should not change cleared offset
        commandHandler.handle(info, commandMessage(1l));
        assertClearedUpTo(11);
    }

    private void assertClearedUpTo(int max) {
        for (int c=0; c<=max; c++) { 
            assertThat(commandPoller.isCleared(c), equalTo(true));
        }
        assertThat(commandPoller.isCleared(max+1), equalTo(false));

    }

    @Test
    public void testIgnoreInvalidCommand() throws DistributionException, InterruptedException, IOException {
        createCommandPoller(true);
        
        CommandMessage message = CommandMessage.newBuilder(commandMessage(10l)).clearClearCommand().build();
        commandHandler.handle(info, message);
        assertClearedUpTo(-1);
    }
    
    @Test
    public void testEditable() throws DistributionException, InterruptedException, IOException {
        createCommandPoller(true);
        
        commandPoller.close();
        
        verify(poller).close();
    }
    
    @Test
    public void testNotEditable() throws DistributionException, InterruptedException, IOException {
        createCommandPoller(false);
        
        commandPoller.close();
        
        verify(poller, never()).close();
    }

    private void assertSkipped() {
        assertThat(commandPoller.isCleared(1), equalTo(false));
    }

    private CommandMessage commandMessage(long offset) {
        return commandMessage(SUB_SLING_ID, SUB_AGENT_NAME, offset);
    }
    
    private CommandMessage commandMessage(String subSlingId, String subAgentName, long offset) {
        ClearCommand command = ClearCommand.newBuilder()
                .setOffset(offset)
                .build();
        return CommandMessage.newBuilder()
                .setClearCommand(command)
                .setSubAgentName(subAgentName)
                .setSubSlingId(subSlingId)
                .build();
    }

    private void createCommandPoller(boolean editable) {
        when(clientProvider.createPoller(
                Mockito.anyString(),
                Mockito.eq(Reset.earliest), 
                handlerCaptor.capture()))
            .thenReturn(poller);
        commandPoller = new CommandPoller(clientProvider, topics, SUB_SLING_ID, SUB_AGENT_NAME, editable);
        if (editable) {
            commandHandler = handlerCaptor.getValue().getHandler();
        }
    }

}
