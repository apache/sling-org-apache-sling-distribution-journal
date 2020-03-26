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

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.journal.service.subscriber.LocalStore;
import org.apache.sling.testing.resourceresolver.MockResourceResolverFactory;
import org.junit.Test;

public class LocalStoreTest {

    @Test
    public void storeConsecutiveOffsets() throws InterruptedException, PersistenceException, LoginException {
        MockResourceResolverFactory resolverFactory = new MockResourceResolverFactory();
        ResourceResolver resourceResolver = resolverFactory.getServiceResourceResolver(null);
        LocalStore offsetStore = new LocalStore(resolverFactory, "packages", "store1");
        assertThat(offsetStore.load("offset", -1L), equalTo(-1l));
        offsetStore.store(resourceResolver, "offset", 2l);
        resourceResolver.commit();
        assertThat(offsetStore.load("offset", -1L), equalTo(2l));
        offsetStore.store(resourceResolver, "offset", 3l);
        resourceResolver.commit();
        assertThat(offsetStore.load("offset", -1L), equalTo(3l));
    }

    @Test
    public void commitExternally() throws Exception {
        MockResourceResolverFactory resolverFactory = new MockResourceResolverFactory();
        ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null);
        LocalStore offsetStore = new LocalStore(resolverFactory, "packages", "store3");
        offsetStore.store(resolver, "key1", "value1");
        assertNull(offsetStore.load("key1", null));
        resolver.commit();
        assertEquals("value1", offsetStore.load("key1", null));
    }

    @Test
    public void storeStatus() throws Exception {
        MockResourceResolverFactory resolverFactory = new MockResourceResolverFactory();
        ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null);
        LocalStore statusStore = new LocalStore(resolverFactory, "statuses", "store2");

        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", false);

        statusStore.store(resolver, map);
        resolver.commit();

        assertEquals("value1", statusStore.load("key1", null));
        assertEquals(false, statusStore.load("key2", null));
    }

    @Test
    public void updateStoredStatus() throws Exception {
        MockResourceResolverFactory resolverFactory = new MockResourceResolverFactory();
        ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null);
        LocalStore statusStore = new LocalStore(resolverFactory, "statuses", "store4");

        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", false);

        statusStore.store(resolver, map);
        resolver.commit();

        statusStore.store(resolver, "key2", true);
        resolver.commit();

        assertEquals("value1", statusStore.load("key1", null));
        assertEquals(true, statusStore.load("key2", null));
    }
}
