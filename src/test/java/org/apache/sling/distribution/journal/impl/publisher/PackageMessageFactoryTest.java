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

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.BinaryStore;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.packaging.DistributionPackageInfo;
import org.apache.sling.settings.SlingSettingsService;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.sling.distribution.DistributionRequestType.ADD;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.osgi.util.converter.Converters.standardConverter;

@RunWith(MockitoJUnitRunner.class)
public class PackageMessageFactoryTest {

    private static final String PUB_AGENT_NAME = "pubAgentName";

    @Mock
    private DistributionPackageBuilder packageBuilder;

    @Mock
    private DistributionPackage distributionPackage;

    @Mock
    private SlingSettingsService slingSettingsService;

    @Mock
    private BinaryStore binaryStore;

    private final ResourceResolverFactory resolverFactory = new MockResourceResolverFactory();

    @InjectMocks
    private final PackageMessageFactory factory = new PackageMessageFactory();


    @Before
    public void before() throws DistributionException {
        when(packageBuilder.createPackage(any(ResourceResolver.class), any(DistributionRequest.class)))
                .thenReturn(distributionPackage);
        when(slingSettingsService.getSlingId())
                .thenReturn(UUID.randomUUID().toString());

        when(distributionPackage.getInfo())
                .thenReturn(buildPackageInfo());
    }

    @Test(expected = DistributionException.class)
    public void testAddPkgLengthTooLarge() throws Exception {
        PackageFactoryConfiguration config = standardConverter()
                .convert(singletonMap("maxPackageSize", 1000))
                .to(PackageFactoryConfiguration.class);
        factory.activate(config);
        when(distributionPackage.createInputStream())
                .thenReturn(new ByteArrayInputStream(new byte[(int)config.maxPackageSize() + 1]));
        DistributionRequest request = new SimpleDistributionRequest(ADD, "/some/path");
        factory.create(packageBuilder, resolverFactory.getServiceResourceResolver(null), PUB_AGENT_NAME, request);
    }

    @Test
    public void testAddPkgNoLimit() throws Exception {
        PackageFactoryConfiguration config = standardConverter()
                .convert(emptyMap())
                .to(PackageFactoryConfiguration.class);
        factory.activate(config);
        when(distributionPackage.createInputStream())
                .thenReturn(new ByteArrayInputStream(new byte[10_000_000]));
        DistributionRequest request = new SimpleDistributionRequest(ADD, "/some/path");
        assertNotNull(factory.create(packageBuilder, resolverFactory.getServiceResourceResolver(null), PUB_AGENT_NAME, request));
    }

    private DistributionPackageInfo buildPackageInfo() {
        Map<String, Object> props = new HashMap<>();
        props.put(DistributionPackageInfo.PROPERTY_REQUEST_PATHS, "/some/path");
        props.put(DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS, "/some/path");
        return new DistributionPackageInfo("packageType", props);
    }

}