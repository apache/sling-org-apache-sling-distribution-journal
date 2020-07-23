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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueueCacheSeederTest {

    @Captor
    private ArgumentCaptor<PackageMessage> pkgMsgCaptor;

    @Mock
    private MessageSender<PackageMessage> sender;

    private QueueCacheSeeder seeder;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        doNothing().when(sender).send(pkgMsgCaptor.capture());
        seeder = new QueueCacheSeeder(sender);
    }

    @Test
    public void testSeeding() throws IOException {
        seeder.startSeeding();
        
        verify(sender, timeout(1000)).send(Mockito.anyObject());
    }

    @After
    public void after() {
        seeder.close();
    }

}
