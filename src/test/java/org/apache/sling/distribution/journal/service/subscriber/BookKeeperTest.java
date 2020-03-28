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
package org.apache.sling.distribution.journal.service.subscriber;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.impl.subscriber.SubscriberTest;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.service.event.EventAdmin;

@RunWith(MockitoJUnitRunner.class)
public class BookKeeperTest {

    private static final int COMMIT_AFTER_NUM_SKIPPED = 10;

    private ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();

    private SubscriberMetrics subscriberMetrics = new SubscriberMetrics();

    @Mock
    private PackageHandler packageHandler;

    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private Consumer<PackageStatusMessage> sender;
    
    @Mock
    BundleContext context;

    private BookKeeper bookKeeper;

    @Before
    public void before() {
        SubscriberIdle subscriberIdle = new SubscriberIdle(context, 300);
        bookKeeper = new BookKeeper(resolverFactory, subscriberMetrics,  packageHandler, subscriberIdle, eventAdmin, sender,
                "subAgentName", "subSlingId", true, 10);
        sem = new Semaphore(0);
    }

    @Test
    public void testOnlyEveryTenthSkippedPackageOffsetStored() throws InterruptedException, PersistenceException, LoginException {
        for (int c = 0; c < COMMIT_AFTER_NUM_SKIPPED; c++) {
            bookKeeper.skipPackage(c);
            assertThat(bookKeeper.loadOffset(), equalTo(-1l));
        }
        for (int c = COMMIT_AFTER_NUM_SKIPPED; c < COMMIT_AFTER_NUM_SKIPPED * 2; c++) {
            bookKeeper.skipPackage(c);
            assertThat(bookKeeper.loadOffset(), equalTo(10l));
        }
        for (int c = COMMIT_AFTER_NUM_SKIPPED * 2; c < COMMIT_AFTER_NUM_SKIPPED * 3; c++) {
            bookKeeper.skipPackage(c);
            assertThat(bookKeeper.loadOffset(), equalTo(20l));
        }
    }
    
    @Test
    public void testReadyWhenWatingForPrecondition() {
        
        MessageInfo info = new TestMessageInfo("", 1, 0, 0);
        PackageMessage message = SubscriberTest.BASIC_ADD_PACKAGE;
        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                bookKeeper.processPackage(info, message, this::waitForSemaphore);
            } catch (Exception e) {
            }
        });
        await("Should report ready").until(bookKeeper.subscriberIdle::isReady);
        sem.release();
    }

    public boolean waitForSemaphore(long offset, PackageMessage msg) {
        try {
            return sem.tryAcquire(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    private Semaphore sem;
}
