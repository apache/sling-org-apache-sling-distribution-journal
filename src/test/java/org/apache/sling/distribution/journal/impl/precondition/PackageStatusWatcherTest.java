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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Closeable;

import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.shared.Topics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

public class PackageStatusWatcherTest {


    final static String TOPIC_NAME = Topics.STATUS_TOPIC;

    private static final String SUB1_SLING_ID = "sub1sling";
    private static final String SUB1_AGENT_NAME = "sub1agent";

    private static final String PUB1_AGENT_NAME = "pub1agent";

    @Mock
    MessagingProvider provider;

    @Spy
    Topics topics = new Topics();

    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageStatusMessage>> adapterCaptor;

    PackageStatusWatcher statusWatcher;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        when(provider.createPoller(
                eq(TOPIC_NAME),
                eq(Reset.earliest),
                adapterCaptor.capture()))
                .thenReturn(mock(Closeable.class));

        statusWatcher = new PackageStatusWatcher(provider, topics);

    }

    @Test
    public void testStatusWatcherRemoveFailed() {
        generateStatusMessagesFromTo(10, 50, Status.REMOVED_FAILED);

        assertPackageStatus("Offset is lower than lowest package offset from status messages. So we assume imported.", 1000, Status.IMPORTED);
        assertPackageStatus("We should have got explicit status here", 1010, Status.REMOVED_FAILED);
        assertPackageStatus("Status should not yet have arrived", 1051, null);
    }
    
    @Test
    public void testStatusWatcherStatusMessageMissing() {
        generateStatusMessagesFromTo(1, 1, Status.IMPORTED);

        assertPackageStatus("", 1001, Status.IMPORTED);
        assertPackageStatus("This package status should be missing. So publish would wait", 1002, null);
        
        generateStatusMessagesFromTo(3, 3, Status.IMPORTED);
        assertPackageStatus("", 1003, Status.IMPORTED);
        assertPackageStatus("As we got a status message for a higher package offset this should allow import now", 1002, Status.IMPORTED);
    }


    void generateStatusMessagesFromTo(int begin, int end, Status status) {
        MessageHandler<PackageStatusMessage> handler = adapterCaptor.getValue().getHandler();
        for (int i=begin; i<=end; i++) {
            handler.handle(new TestMessageInfo(TOPIC_NAME, 0, i, 0l),
                    createStatusMessage(i, status));
        }
    }

    PackageStatusMessage createStatusMessage(int i, Status status) {
        return PackageStatusMessage.builder()
                .subSlingId(SUB1_SLING_ID)
                .subAgentName(SUB1_AGENT_NAME)
                .pubAgentName(PUB1_AGENT_NAME)
                .offset(1000 + i)
                .status(status)
                .build();

    }

    void assertPackageStatus(String msg, long pkgOffset, Status expectedStatus) {
        Status status = statusWatcher.getStatus(SUB1_AGENT_NAME, pkgOffset);
        assertEquals(msg, expectedStatus, status);
    }

}
