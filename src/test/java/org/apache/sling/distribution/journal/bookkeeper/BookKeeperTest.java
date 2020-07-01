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
package org.apache.sling.distribution.journal.bookkeeper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.function.Consumer;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.service.event.EventAdmin;

@RunWith(MockitoJUnitRunner.class)
public class BookKeeperTest {

    private static final int COMMIT_AFTER_NUM_SKIPPED = 10;

    private ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();

    @Mock
    private DistributionMetricsService distributionMetricsService;

    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private Consumer<PackageStatusMessage> sender;

    private BookKeeper bookKeeper;

    @Mock
    private PackageHandler packageHandler;

    @Mock
    private DistributionPackageBuilder packageBuilder;

    @Before
    public void before() {
        BookKeeperConfig bkConfig = new BookKeeperConfig("subAgentName", "subSlingId", true, 10, PackageHandling.Extract);
        bookKeeper = new BookKeeper(resolverFactory, distributionMetricsService, packageHandler, eventAdmin, sender, bkConfig);
    }

    @Test
    public void testOnlyEveryTenthSkippedPackageOffsetStored() throws InterruptedException, PersistenceException, LoginException {
        for (int c = 0; c < COMMIT_AFTER_NUM_SKIPPED; c++) {
            bookKeeper.skipPackage(c);
            assertThat(bookKeeper.loadOffset(), equalTo(-1L));
        }
        for (int c = COMMIT_AFTER_NUM_SKIPPED; c < COMMIT_AFTER_NUM_SKIPPED * 2; c++) {
            bookKeeper.skipPackage(c);
            assertThat(bookKeeper.loadOffset(), equalTo(10L));
        }
        for (int c = COMMIT_AFTER_NUM_SKIPPED * 2; c < COMMIT_AFTER_NUM_SKIPPED * 3; c++) {
            bookKeeper.skipPackage(c);
            assertThat(bookKeeper.loadOffset(), equalTo(20L));
        }
    }

}
