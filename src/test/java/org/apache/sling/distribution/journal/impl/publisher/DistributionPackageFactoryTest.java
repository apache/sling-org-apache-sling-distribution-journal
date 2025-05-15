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

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.osgi.util.converter.Converters.standardConverter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.BinaryStore;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.packaging.DistributionPackageInfo;
import org.apache.sling.settings.SlingSettingsService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DistributionPackageFactoryTest {

    @Mock
    private DistributionPackageBuilder packageBuilder;

    @Mock
    private ResourceResolver resourceResolver;

    @Mock
    private BinaryStore binaryStore;

    @Mock
    private SlingSettingsService slingSettings;

    @InjectMocks
    private PackageMessageFactory publisher;

    @Before
    public void before() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        when(packageBuilder.getType()).thenReturn("journal");
        when(slingSettings.getSlingId()).thenReturn("pub1sling");

        PackageFactoryConfiguration config = standardConverter()
                .convert(emptyMap())
                .to(PackageFactoryConfiguration.class);
        publisher.activate(config);

        when(resourceResolver.getUserID()).thenReturn("testUser");
    }

    @Test
    public void testEmpty() throws DistributionException {
        DistributionRequest add = new SimpleDistributionRequest(DistributionRequestType.ADD, "/test");
        DistributionRequest delete = new SimpleDistributionRequest(DistributionRequestType.DELETE, "/test");

        when(packageBuilder.createPackage(eq(resourceResolver), any())).thenReturn(null);

        assertNull(publisher.create(packageBuilder, resourceResolver, "pub1agent1", add));
        assertNull(publisher.create(packageBuilder, resourceResolver, "pub1agent1", delete));
    }
    
    @Test
    public void testAdd() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "/test");

        DistributionPackage pkg = mock(DistributionPackage.class);
        when(binaryStore.put(anyString(), any(), anyLong())).thenReturn(null);

        when(pkg.createInputStream()).thenReturn(new ByteArrayInputStream(new byte[] { 0x00 }));
        when(pkg.getSize()).thenReturn(1L);
        when(pkg.getId()).thenReturn("myid");
        Map<String, Object> props = new HashMap<>();
        props.put(DistributionPackageInfo.PROPERTY_REQUEST_PATHS, request.getPaths());
        props.put(DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS, "/test2");
        DistributionPackageInfo info = new DistributionPackageInfo("journal",
                props);
        when(pkg.getInfo()).thenReturn(info);
        when(packageBuilder.createPackage(resourceResolver, request)).thenReturn(pkg);

        PackageMessage sent = publisher.create(packageBuilder, resourceResolver, "pub1agent1", request);
        
        assertThat(sent.getPkgBinary(), notNullValue());
        assertThat(sent.getPkgLength(), equalTo(1L));
        assertThat(sent.getReqType(), equalTo(ReqType.ADD));
        assertThat(sent.getPkgType(), equalTo("journal"));
        assertThat(sent.getPaths(), contains("/test"));
        assertThat(sent.getDeepPaths(), contains("/test2"));
    }

    @Test
    public void testAddBig() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "/test");

        DistributionPackage pkg = mock(DistributionPackage.class);
        when(binaryStore.put(anyString(), any(), anyLong())).thenReturn("emptyId");

        when(pkg.createInputStream()).thenReturn(new ByteArrayInputStream(new byte[819200]));
        when(pkg.getSize()).thenReturn(819200L);
        when(pkg.getId()).thenReturn("myid");
        Map<String, Object> props = new HashMap<>();
        props.put(DistributionPackageInfo.PROPERTY_REQUEST_PATHS, request.getPaths());
        props.put(DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS, "/test2");
        DistributionPackageInfo info = new DistributionPackageInfo("journal", props);
        when(pkg.getInfo()).thenReturn(info);
        when(packageBuilder.createPackage(resourceResolver, request)).thenReturn(pkg);

        PackageMessage sent = publisher.create(packageBuilder, resourceResolver, "pub1agent1", request);

        assertThat(sent.getPkgBinaryRef(), equalTo("emptyId"));
        assertThat(sent.getPkgLength(), equalTo(819200L));
        assertThat(sent.getReqType(), equalTo(ReqType.ADD));
        assertThat(sent.getPkgType(), equalTo("journal"));
        assertThat(sent.getPaths(), contains("/test"));
        assertThat(sent.getDeepPaths(), contains("/test2"));
    }

    @Test
    public void testDelete() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.DELETE, "/test");

        DistributionPackage pkg = mock(DistributionPackage.class);
        when(pkg.getSize()).thenReturn(0L);
        when(pkg.getId()).thenReturn("myid");
        Map<String, Object> props = new HashMap<>();
        props.put(DistributionPackageInfo.PROPERTY_REQUEST_PATHS, request.getPaths());
        props.put(DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS, "/test");
        DistributionPackageInfo info = new DistributionPackageInfo("journal", props);
        when(pkg.getInfo()).thenReturn(info);
        when(packageBuilder.createPackage(resourceResolver, request)).thenReturn(pkg);

        PackageMessage sent = publisher.create(packageBuilder, resourceResolver, "pub1agent1", request);
        assertThat(sent.getReqType(), equalTo(ReqType.DELETE));
        assertThat(sent.getPkgBinary(), nullValue());
        assertThat(sent.getPkgLength(), equalTo(0L));
        assertThat(sent.getPaths(), contains("/test"));
    }

    @Test
    public void testInvalidate() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.INVALIDATE, "/test");

        PackageMessage sent = publisher.create(packageBuilder, resourceResolver, "pub1agent1", request);
        assertThat(sent.getReqType(), equalTo(ReqType.INVALIDATE));
        assertThat(sent.getPkgBinary(), nullValue());
        assertThat(sent.getPkgLength(), equalTo(0L));
        assertThat(sent.getPaths(), contains("/test"));
    }
}
