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

import org.apache.sling.distribution.journal.impl.shared.TestMessageInfo;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import com.google.common.collect.ImmutableMap;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
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
import org.osgi.util.converter.Converters;

import java.io.Closeable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StagingPreconditionTest {

    private static final String SUB1_SLING_ID = "sub1sling";
    private static final String GP_SUB1_AGENT_NAME = "gpsub1agent";
    private static final String PUB1_AGENT_NAME = "pub1agent";

    @Mock
    MessagingProvider clientProvider;

    @Spy
    Topics topics = new Topics();


    @Spy
    private ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();


    @Captor
    private ArgumentCaptor<HandlerAdapter<PackageStatusMessage>> statusCaptor;

    @InjectMocks
    StagingPrecondition precondition;

    private MessageHandler<PackageStatusMessage> statusHandler;


    @Before
    public void before() {
        Awaitility.setDefaultPollDelay(Duration.ZERO);
        Awaitility.setDefaultPollInterval(Duration.ONE_HUNDRED_MILLISECONDS);
        MockitoAnnotations.initMocks(this);

        StagingPrecondition.Configuration config = Converters.standardConverter()
                .convert(ImmutableMap.of("subAgentName", GP_SUB1_AGENT_NAME)).to(StagingPrecondition.Configuration.class);
        when(clientProvider.createPoller(
                Mockito.anyString(),
                Mockito.eq(Reset.earliest),
                statusCaptor.capture()))
                .thenReturn(mock(Closeable.class));

        precondition.activate(config);
        statusHandler = statusCaptor.getValue().getHandler();
    }

    @Test
    public void testStatus() {
        statusHandler.handle(new TestMessageInfo("", 1, 0, 0),
                createMessage(1000, PackageStatusMessage.Status.REMOVED_FAILED));

        statusHandler.handle(new TestMessageInfo("", 1, 0, 0),
                createMessage(1001, PackageStatusMessage.Status.REMOVED));

        statusHandler.handle(new TestMessageInfo("", 1, 0, 0),
                createMessage(1002, PackageStatusMessage.Status.IMPORTED));

        assertFalse(precondition.canProcess(1000, 1));
        assertFalse(precondition.canProcess(1001, 1));
        assertTrue(precondition.canProcess(1002, 1));
        assertTrue(precondition.canProcess(1002, 1));
        assertTrue(precondition.canProcess(1002,1 ));
    }

    @Test
    public void testClearCache() {
        statusHandler.handle(new TestMessageInfo("", 1, 0, 0),
                createMessage(1000, PackageStatusMessage.Status.REMOVED_FAILED));

        statusHandler.handle(new TestMessageInfo("", 1, 0, 0),
                createMessage(1001, PackageStatusMessage.Status.REMOVED));

        assertFalse(precondition.canProcess(1000, 1));
        assertFalse(precondition.canProcess(1001,1 ));
        assertThrows(1000);
        statusHandler = statusCaptor.getValue().getHandler();

        statusHandler.handle(new TestMessageInfo("", 1, 0, 0),
                createMessage(1000, PackageStatusMessage.Status.REMOVED_FAILED));

        assertFalse(precondition.canProcess(1000, 1));
    }


    void assertThrows(long offset) {
        try {
            precondition.canProcess(offset,1 );
            fail("it must throw");
        } catch (IllegalStateException e) {

        }
    }

    PackageStatusMessage createMessage(long offset, PackageStatusMessage.Status status) {
        return PackageStatusMessage.newBuilder()
                .setSubSlingId(SUB1_SLING_ID)
                .setSubAgentName(GP_SUB1_AGENT_NAME)
                .setPubAgentName(PUB1_AGENT_NAME)
                .setOffset(offset)
                .setStatus(status)
                .build();
    }
}
