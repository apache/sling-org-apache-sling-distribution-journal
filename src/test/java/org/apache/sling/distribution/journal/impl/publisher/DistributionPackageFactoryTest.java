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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.packaging.DistributionPackageInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class DistributionPackageFactoryTest {

    @Mock
    private DistributionPackageBuilder packageBuilder;

    @Mock
    private ResourceResolver resourceResolver;

    private PackageMessageFactory publisher;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        when(packageBuilder.getType()).thenReturn("journal");
        publisher = new PackageMessageFactory("pub1sling");
        when(resourceResolver.getUserID()).thenReturn("testUser");
    }
    
    @Test
    public void testAdd() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "/test");

        DistributionPackage pkg = mock(DistributionPackage.class);
        when(pkg.createInputStream()).thenReturn(new ByteArrayInputStream(new byte[] {}));
        when(pkg.getId()).thenReturn("myid");
        Map<String, Object> props = new HashMap<>();
        props.put(DistributionPackageInfo.PROPERTY_REQUEST_PATHS, request.getPaths());
        props.put(DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS, "/test2");
        DistributionPackageInfo info = new DistributionPackageInfo("journal",
                props);
        when(pkg.getInfo()).thenReturn(info);
        when(packageBuilder.createPackage(Mockito.eq(resourceResolver), Mockito.eq(request))).thenReturn(pkg);

        PackageMessage sent = publisher.create(packageBuilder, resourceResolver, "pub1agent1", request);
        
        assertThat(sent.getPkgBinary(), notNullValue());
        assertThat(sent.getPkgLength(), equalTo(0L));
        assertThat(sent.getReqType(), equalTo(ReqType.ADD));
        assertThat(sent.getPkgType(), equalTo("journal"));
        assertThat(sent.getPaths(), contains("/test"));
        assertThat(sent.getDeepPaths(), contains("/test2"));
    }
    
    @Test
    public void testDelete() throws DistributionException, IOException {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.DELETE, "/test");

        PackageMessage sent = publisher.create(packageBuilder, resourceResolver, "pub1agent1", request);
        assertThat(sent.getReqType(), equalTo(ReqType.DELETE));
        assertThat(sent.getPkgBinary(), nullValue());
        assertThat(sent.getPkgLength(), equalTo(0L));
        assertThat(sent.getPaths(), contains("/test"));
        
    }
}
