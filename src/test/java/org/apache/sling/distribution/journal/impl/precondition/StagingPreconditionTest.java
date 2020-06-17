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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
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
    private static final Long OFFSET_NOT_PRESENT = 111111L;

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
    
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalTimeout() throws InterruptedException, TimeoutException {
        precondition.canProcess(GP_SUB1_AGENT_NAME, OFFSET_NOT_PRESENT, -1);
    }
    
    @Test(expected = TimeoutException.class)
    public void testNotYetProcessed() throws InterruptedException, TimeoutException {
        simulateMessage(OTHER_AGENT, 1002, PackageStatusMessage.Status.IMPORTED);
        boolean res = precondition.canProcess(OTHER_AGENT, OFFSET_NOT_PRESENT, 1);
        assertThat(res, equalTo(true));

        // We got no package for this agent. So this should time out
        precondition.canProcess(GP_SUB1_AGENT_NAME, OFFSET_NOT_PRESENT, 1);
    }
    
    @Test
    public void testDeactivateDuringCanProcess() {
        AtomicReference<Throwable> exHolder = new AtomicReference<>();
        Thread th = new Thread(() -> {
            try {
                precondition.canProcess(GP_SUB1_AGENT_NAME, OFFSET_NOT_PRESENT, 2);
            } catch (Throwable t) {
                exHolder.set(t);
            }
        });
        th.start();
        precondition.deactivate();
        Throwable ex = Awaitility.await().until(exHolder::get, notNullValue());
        assertThat(ex, instanceOf(IllegalStateException.class));
    }
    
    @Test(expected = TimeoutException.class)
    public void testCleanup() throws InterruptedException, TimeoutException {
        simulateMessage(GP_SUB1_AGENT_NAME, 1002, PackageStatusMessage.Status.IMPORTED);
        assertTrue(precondition.canProcess(GP_SUB1_AGENT_NAME, 1002, 1));
        
        // Cleanup
        precondition.run();
        
        // Should time out because after cleanup message is not present anymore
        precondition.canProcess(GP_SUB1_AGENT_NAME, 1002, 1);
    }
    
    @Test
    public void testStatus() throws InterruptedException, TimeoutException {
        simulateMessage(GP_SUB1_AGENT_NAME, 1000, PackageStatusMessage.Status.REMOVED_FAILED);
        simulateMessage(GP_SUB1_AGENT_NAME, 1001, PackageStatusMessage.Status.REMOVED);
        simulateMessage(GP_SUB1_AGENT_NAME, 1002, PackageStatusMessage.Status.IMPORTED);

        assertFalse(precondition.canProcess(GP_SUB1_AGENT_NAME, 1000, 1));
        assertFalse(precondition.canProcess(GP_SUB1_AGENT_NAME, 1001, 1));
        assertTrue(precondition.canProcess(GP_SUB1_AGENT_NAME, 1002, 1));
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
