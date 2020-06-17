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
package org.apache.sling.distribution.journal.impl.shared;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.jcr.Binary;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.commons.jackrabbit.SimpleReferenceBinary;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PackageBrowserTest {
    private static final String DATA = "test";

    @Mock
    ResourceResolverFactory resolverFactory;

    @Mock
    ResourceResolver resolver;

    @Mock
    Session session;

    @Mock
    ValueFactory valueFactory;
     
    @Mock
    Value value;

    @InjectMocks
    PackageBrowser packageBrowser;

    @Mock
    Binary binary;

    @Test
    public void testWriteTo() throws Exception {
        when(resolverFactory.getServiceResourceResolver(Mockito.any())).thenReturn(resolver);
        when(resolver.adaptTo(Session.class)).thenReturn(session);
        when(session.getValueFactory()).thenReturn(valueFactory);
        when(valueFactory.createValue(Mockito.any(SimpleReferenceBinary.class))).thenReturn(value);
        when(value.getBinary()).thenReturn(binary);
        ByteArrayInputStream is = new ByteArrayInputStream(DATA.getBytes(StandardCharsets.UTF_8));
        when(binary.getStream()).thenReturn(is);
        PackageMessage pkgMsg = createPackageMsg(0L);
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        packageBrowser.writeTo(pkgMsg, bao);
        String resultSt = bao.toString("utf-8");
        assertThat(resultSt,  equalTo(DATA));
    }

    private PackageMessage createPackageMsg(long offset) throws Exception {
        return PackageMessage.builder()
                .pubSlingId("")
                .reqType(ReqType.ADD)
                .paths(Arrays.asList("/content"))
                .pkgId("pkgid")
                .pkgType("some_type")
                .pkgBinaryRef("myref").build();
    }
}
