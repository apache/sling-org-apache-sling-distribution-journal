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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PublisherConfigurationAvailableTest {

    @InjectMocks
    private PublisherConfigurationAvailable pca;

    @Mock
    private BundleContext context;

    @Mock
    private ConfigurationAdmin configAdmin;

    @Before
    public void before() throws Exception {
        ServiceRegistration<PublisherConfigurationAvailable> serviceReg = mock(ServiceRegistration.class);
        when(context.registerService(eq(PublisherConfigurationAvailable.class), any(PublisherConfigurationAvailable.class), any()))
                .thenReturn(serviceReg);
        when(configAdmin.listConfigurations(anyString()))
                .thenReturn(null);
        pca.activate(context);
    }

    @After
    public void after() {
        pca.deactivate();
    }

    @Test
    public void testNoConfiguration() {
        assertFalse(pca.isAvailable());
        pca.run();
        assertFalse(pca.isAvailable());
    }

    @Test
    public void testWithZeroConfiguration() throws Exception {
        assertFalse(pca.isAvailable());
        addConfigurations(0);
        pca.run();
        assertFalse(pca.isAvailable());
    }

    @Test
    public void testWithOneConfiguration() throws Exception {
        assertFalse(pca.isAvailable());
        addConfigurations(1);
        pca.run();
        assertTrue(pca.isAvailable());
    }

    @Test
    public void testWithManyConfigurations() throws Exception {
        assertFalse(pca.isAvailable());
        addConfigurations(10);
        pca.run();
        assertTrue(pca.isAvailable());
    }
    
    @Test
    public void testRemainAvailable() throws Exception {
        addConfigurations(1);
        pca.run();
        assertTrue(pca.isAvailable());
        removeAllConfigurations();
        pca.run();
        assertTrue(pca.isAvailable());
    }
    
    private void removeAllConfigurations() throws Exception {
        when(configAdmin.listConfigurations(anyString()))
                .thenReturn(null);
    }

    private void addConfigurations(int nbConfigurations) throws Exception {
        when(configAdmin.listConfigurations(anyString()))
                .thenReturn(new Configuration[nbConfigurations]);
    }

}