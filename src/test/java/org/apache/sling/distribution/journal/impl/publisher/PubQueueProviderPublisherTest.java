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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.impl.queue.PubQueueProviderFactory;
import org.apache.sling.distribution.journal.shared.Topics;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

@RunWith(MockitoJUnitRunner.class)
public class PubQueueProviderPublisherTest {
    
    @Mock
    MessagingProvider messagingProvider;

    @Mock
    private BundleContext context;
    
    @Mock
    PubQueueProviderFactory pubQueueProviderFactory;
    
    @Spy
    Topics topics = new Topics();

    @InjectMocks
    private PubQueueProviderPublisher pubQueueProviderPublisher;

    @Mock
    private PubQueueProvider pubQueueProvider;

    @Mock
    private ServiceRegistration<PubQueueProvider> reg;

    @Test
    public void testCycle() throws IOException {
        when(pubQueueProviderFactory.create(Mockito.any())).thenReturn(pubQueueProvider);
        when(context.registerService(Mockito.eq(PubQueueProvider.class), Mockito.eq(pubQueueProvider), Mockito.any()))
                .thenReturn(reg);
        pubQueueProviderPublisher.activate(context);
        
        pubQueueProviderPublisher.deactivate();
        verify(pubQueueProvider).close();
        verify(reg).unregister();
    }
}
