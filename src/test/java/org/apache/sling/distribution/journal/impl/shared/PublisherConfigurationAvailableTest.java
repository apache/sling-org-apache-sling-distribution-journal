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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Dictionary;
import java.util.Hashtable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationException;

@RunWith(MockitoJUnitRunner.class)
public class PublisherConfigurationAvailableTest {

    private PublisherConfigurationAvailable configAvailable;
    
    @Mock
    private BundleContext context;

    @Mock
    private ServiceRegistration<PublisherConfigurationAvailable> reg;

    @Before
    public void before() {
        configAvailable = new PublisherConfigurationAvailable();
        configAvailable.activate(context);
    }

    @Test
    public void testNoConfig() {
        assertThat(configAvailable.isAvailable(), equalTo(false));
    }
    
    @Test
    public void testConfig() throws ConfigurationException {
        when(context.registerService(Mockito.eq(PublisherConfigurationAvailable.class), Mockito.eq(configAvailable), Mockito.anyObject()))
            .thenReturn(reg);
        
        Dictionary<String, ?> properties = new Hashtable<>();
        configAvailable.updated("any", properties);
        assertThat(configAvailable.isAvailable(), equalTo(true));
        
        configAvailable.deleted("any");
        assertThat(configAvailable.isAvailable(), equalTo(true));
    }

}
