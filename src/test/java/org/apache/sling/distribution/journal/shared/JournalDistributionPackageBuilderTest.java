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
package org.apache.sling.distribution.journal.shared;

import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.serialization.DistributionContentSerializer;
import org.apache.sling.distribution.serialization.DistributionContentSerializerProvider;
import org.apache.sling.distribution.serialization.ExportSettings;
import org.apache.sling.distribution.serialization.ImportSettings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.util.UUID;

import static java.util.Map.of;
import static org.apache.sling.distribution.DistributionRequestType.ADD;
import static org.apache.sling.distribution.DistributionRequestType.DELETE;
import static org.apache.sling.distribution.DistributionRequestType.PULL;
import static org.apache.sling.distribution.journal.shared.JournalDistributionPackageBuilder.Configuration;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.osgi.util.converter.Converters.standardConverter;

@RunWith(MockitoJUnitRunner.class)
public class JournalDistributionPackageBuilderTest {

    @Mock
    private DistributionContentSerializerProvider serializerProvider;

    @Mock
    private DistributionContentSerializer serializer;

    @Mock
    private ResourceResolver resolver;

    private JournalDistributionPackageBuilder builder;

    @Before
    public void setUp() throws Exception {
        when(serializerProvider.build(anyString(), any(ExportSettings.class), any(ImportSettings.class)))
                .thenReturn(serializer);
        Configuration config = standardConverter().convert(of("/some/path", "/some/other/path")).to(Configuration.class);
        builder = new JournalDistributionPackageBuilder(config, serializerProvider);
        assertEquals("journal-distribution", builder.getType());
    }

    @Test
    public void testInstallAddPackage() throws Exception {
        DistributionRequest request = new SimpleDistributionRequest(ADD, "/some/path");
        DistributionPackage pkg = builder.createPackage(resolver, request);
        assertTrue(builder.installPackage(resolver, pkg));
    }

    @Test
    public void testInstallDeletePackage() throws Exception {
        DistributionRequest request = new SimpleDistributionRequest(DELETE, "/some/path");
        DistributionPackage pkg = builder.createPackage(resolver, request);
        assertTrue(builder.installPackage(resolver, pkg));
    }

    @Test
    public void testInstallUnsupportedPackage() throws DistributionException {
        DistributionRequest request = new SimpleDistributionRequest(PULL, "/some/path");
        DistributionPackage pkg = builder.createPackage(resolver, request);
        assertTrue(builder.installPackage(resolver, pkg));
    }

    @Test
    public void testCreatePackage() throws Exception {
        String[] paths = {"/some/path", "/some/other/path"};
        DistributionRequest request = new SimpleDistributionRequest(ADD, paths);
        DistributionPackage pkg = builder.createPackage(resolver, request);
        assertEquals("journal-distribution", pkg.getInfo().getType());
        assertEquals(ADD, pkg.getInfo().getRequestType());
        assertEquals(0, pkg.getSize());
        assertArrayEquals(paths, pkg.getInfo().getPaths());
    }

    @Test(expected = DistributionException.class)
    public void testReadPackage() throws Exception {
        builder.readPackage(resolver, new ByteArrayInputStream(new byte[0]));
    }

    @Test(expected = DistributionException.class)
    public void testGetPackage() throws Exception {
        builder.getPackage(resolver, UUID.randomUUID().toString());
    }

    @Test(expected = DistributionException.class)
    public void testInstallPackageFromStream() throws Exception {
        builder.installPackage(resolver, new ByteArrayInputStream(new byte[0]));
    }
}