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
package org.apache.sling.distribution.journal.impl.precondition;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.util.concurrent.TimeoutException;

import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.precondition.Precondition.Decision;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.shared.Topics;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

public class StagingPreconditionTest {

    private static final String OTHER_AGENT = "other agent";
    private static final String SUB1_SLING_ID = "sub1sling";
    private static final String GP_SUB1_AGENT_NAME = "gpsub1agent";
    private static final String PUB1_AGENT_NAME = "pub1agent";
    private static final Long OFFSET_NOT_PRESENT = 111111l;

    @Mock
    private MessagingProvider clientProvider;

    @Spy
    private Topics topics = new Topics();

    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageStatusMessage>> statusCaptor;

    @InjectMocks
    private StagingPrecondition precondition;

    private MessageHandler<PackageStatusMessage> statusHandler;

    @Before
    public void before() {
        Awaitility.setDefaultPollDelay(Duration.ZERO);
        Awaitility.setDefaultPollInterval(Duration.ONE_HUNDRED_MILLISECONDS);
        MockitoAnnotations.initMocks(this);

        when(clientProvider.createPoller(
                Mockito.anyString(),
                Mockito.eq(Reset.earliest),
                statusCaptor.capture()))
                .thenReturn(mock(Closeable.class));

        precondition.activate();
        statusHandler = statusCaptor.getValue().getHandler();
    }
    
    @Test
    public void testNotYetProcessed() throws InterruptedException, TimeoutException {
        simulateMessage(OTHER_AGENT, 1002, PackageStatusMessage.Status.IMPORTED);
        Decision res = precondition.canProcess(OTHER_AGENT, OFFSET_NOT_PRESENT);
        assertThat(res, equalTo(Decision.WAIT));

        Decision res2 = precondition.canProcess(GP_SUB1_AGENT_NAME, OFFSET_NOT_PRESENT);
        assertThat(res2, equalTo(Decision.WAIT));

    }
    
    @Test
    public void testCleanup() throws InterruptedException, TimeoutException {
        simulateMessage(GP_SUB1_AGENT_NAME, 1002, PackageStatusMessage.Status.IMPORTED);
        Decision res = precondition.canProcess(GP_SUB1_AGENT_NAME, 1002);
        assertThat(res, equalTo(Decision.ACCEPT));
        
        // Cleanup
        precondition.run();
        
        Decision res2 = precondition.canProcess(GP_SUB1_AGENT_NAME, 1002);
        assertThat(res2, equalTo(Decision.WAIT));
    }
    
    @Test
    public void testStatus() throws InterruptedException, TimeoutException {
        simulateMessage(GP_SUB1_AGENT_NAME, 1000, PackageStatusMessage.Status.REMOVED_FAILED);
        simulateMessage(GP_SUB1_AGENT_NAME, 1001, PackageStatusMessage.Status.REMOVED);
        simulateMessage(GP_SUB1_AGENT_NAME, 1002, PackageStatusMessage.Status.IMPORTED);

        assertThat(precondition.canProcess(GP_SUB1_AGENT_NAME, 1000), equalTo(Decision.SKIP));
        assertThat(precondition.canProcess(GP_SUB1_AGENT_NAME, 1001), equalTo(Decision.SKIP));
        assertThat(precondition.canProcess(GP_SUB1_AGENT_NAME, 1002), equalTo(Decision.ACCEPT));
    }

    private void simulateMessage(String subAgentName, long pkgOffset, PackageStatusMessage.Status status) {
        PackageStatusMessage message = PackageStatusMessage.builder()
                .subSlingId(SUB1_SLING_ID)
                .subAgentName(subAgentName)
                .pubAgentName(PUB1_AGENT_NAME)
                .offset(pkgOffset)
                .status(status)
                .build();
        
        TestMessageInfo offset0 = new TestMessageInfo("", 1, 0, 0);
        statusHandler.handle(offset0, message);
    }
}
