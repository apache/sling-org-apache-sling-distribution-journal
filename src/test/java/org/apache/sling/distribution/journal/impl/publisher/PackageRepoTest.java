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
import java.util.function.Function;

import org.apache.sling.distribution.journal.impl.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.sling.MockSling;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.osgi.framework.BundleContext;

import org.apache.sling.distribution.journal.messages.Messages;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;

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
    private ArgumentCaptor<Messages.PackageMessage> pkgCaptor;

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
    public void testStoreClean() throws DistributionException, IOException, LoginException {
        when(messagingProvider.retrieveOffset(Mockito.anyString(), Mockito.eq(Reset.earliest)))
            .thenReturn(100L, 201L, 201L, 203L);
        when(messagingProvider.retrieveOffset(Mockito.anyString(), Mockito.eq(Reset.latest)))
            .thenReturn(200L, 202L, 202L, 203L);
        when(timer.time())
                .thenReturn(context);
        when(distributionMetricsService.getCleanupPackageDuration())
                .thenReturn(timer);
        when(distributionMetricsService.getCleanupPackageRemovedCount())
                .thenReturn(counter);
        
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            packageRepo.store(resolver, mockPackage());
        }
        
        assertNumNodes(1);
        
        // In a first pass we get current tail offset on all stored packages
        packageRepo.cleanup();
        assertEachNode(this::getOffset, equalTo(200));
        
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            packageRepo.store(resolver, mockPackage());
        }
        assertNumNodes(2);
        
        // We delete the old package and add the offset to the new one
        packageRepo.cleanup();
        assertNumNodes(1);
        
        // As the new head offset is the same the node is not deleted
        packageRepo.cleanup();
        assertNumNodes(1);
        
        // Now the head offset is increased again and the node is deleted
        packageRepo.cleanup();
        assertNumNodes(0);

    }

    private void assertNumNodes(int num) throws LoginException {
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            assertThat(getPackageNodes(resolver).size(), equalTo(num));
        }
    }

    private Integer getOffset(Resource pkg) {
        return pkg.getValueMap().get("offset", Integer.class);
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
    
    public <T> void assertEachNode(Function<Resource, T> func, Matcher<T> matcher) throws LoginException {
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            List<Resource> nodes = getPackageNodes(resolver);
            for (Resource pkg : nodes) {
                assertThat(func.apply(pkg), matcher);
            }
        }
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
