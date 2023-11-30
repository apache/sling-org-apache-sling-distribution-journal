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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.distribution.journal.shared.JMXRegistration;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.sling.MockSling;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;

@RunWith(MockitoJUnitRunner.class)
public class LocalStoreJMXTest {
    
    private LocalStoreJMX resetOffsets;
    
    private ResourceResolverFactory resolverFactory;

    private BundleContext context;
    
    @Before
    public void before() {
        context = MockOsgi.newBundleContext();
        resolverFactory = MockSling.newResourceResolverFactory(ResourceResolverType.JCR_OAK, context);
        resetOffsets = new LocalStoreJMX();
        MockOsgi.injectServices(resetOffsets, context);
        MockOsgi.activate(resetOffsets, context);
    }

    @Test
    public void testResetViaService() throws LoginException, PersistenceException {
        try ( ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            ResourceUtil.getOrCreateResource(resolver, LocalStore.ROOT_PATH, "sling:Folder", "sling:Folder", true);
        }
        LocalStore store = new LocalStore(resolverFactory, "package", "publish");
        store.store("offset", 10l);
        assertThat(store.load("offset", Long.class), equalTo(10l));
        resetOffsets.resetStores();
        try {
            store.load("offset", Long.class);
            fail("NPE expected");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), containsString("Possibly the stores were reset"));
        }
    }
    
    @Test
    public void testResetViaJMX() throws Exception {
        try ( ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            ResourceUtil.getOrCreateResource(resolver, LocalStore.ROOT_PATH, "sling:Folder", "sling:Folder", true);
        }
        LocalStore store = new LocalStore(resolverFactory, "package", "publish");
        store.store("offset", 10l);
        assertThat(store.load("offset", Long.class), equalTo(10l));
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = JMXRegistration.nameOf("offsetReset", "default");
        mbeanServer.invoke(name, "resetStores", null, null);
        try {
            store.load("offset", Long.class);
            fail("NPE expected");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), containsString("Possibly the stores were reset"));
        }
    }
}
