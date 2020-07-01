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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.shared.Topics;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.sling.MockSling;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.osgi.framework.BundleContext;

public class PackageRepoTest {
    
    @Spy
    private BundleContext bundleContext = MockOsgi.newBundleContext();

    @Spy
    ResourceResolverFactory resolverFactory = MockSling.newResourceResolverFactory(ResourceResolverType.JCR_OAK, bundleContext);

    @Mock
    private MessagingProvider messagingProvider;

    @Mock
    private Timer timer;

    @Mock
    private Timer.Context context;

    @Mock
    private Counter counter;

    @Mock
    private DistributionMetricsService distributionMetricsService;

    @Captor
    private ArgumentCaptor<PackageMessage> pkgCaptor;

    @Spy
    private Topics topics = new Topics();
    
    @InjectMocks
    private PackageRepo packageRepo;


    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }
    
    @After
    public void after() {
    }
    
    @Test
    public void testStoreClean() throws DistributionException, IOException, LoginException, InterruptedException {
        when(timer.time())
                .thenReturn(context);
        when(distributionMetricsService.getCleanupPackageDuration())
                .thenReturn(timer);
        when(distributionMetricsService.getCleanupPackageRemovedCount())
                .thenReturn(counter);

        long createTime = System.currentTimeMillis();
        store(mockPackage());
        assertNumNodes(1);
        packageRepo.cleanup(createTime - 1000);
        assertNumNodes(1);
        packageRepo.cleanup(createTime + 1000);
        assertNumNodes(0);
    }

    private void store(DistributionPackage pkg) throws DistributionException, IOException, LoginException {
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            packageRepo.store(resolver, pkg);
        }
    }

    private void assertNumNodes(int num) throws LoginException {
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            assertThat(getPackageNodes(resolver).size(), equalTo(num));
        }
    }

    private List<Resource> getPackageNodes(ResourceResolver resolver) throws LoginException {
        List<Resource> result = new ArrayList<>();
        Resource root = resolver.getResource(PackageRepo.PACKAGES_ROOT_PATH);
        for (Resource type : root.getChildren()) {
            Resource data = type.getChild("data");
            if (data != null) {
                for (Resource pkg : data.getChildren()) {
                    result.add(pkg);
                }
            }
        }
        return result;
    }
    
    private DistributionPackage mockPackage() throws IOException {
        DistributionPackage pkg = mock(DistributionPackage.class);
        when(pkg.getId()).thenReturn(UUID.randomUUID().toString());
        when(pkg.getType()).thenReturn("journal");
        byte[] content = new byte[] {};
        ByteArrayInputStream stream = new ByteArrayInputStream(content);
        when(pkg.createInputStream()).thenReturn(stream);
        return pkg;
    }
    
}
