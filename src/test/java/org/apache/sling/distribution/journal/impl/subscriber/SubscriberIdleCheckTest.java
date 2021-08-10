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
package org.apache.sling.distribution.journal.impl.subscriber;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

@RunWith(MockitoJUnitRunner.class)
public class SubscriberIdleCheckTest {

    private BundleContext context;
    private SubscriberIdleCheck idleCheck;

    @Mock
    private IdleCheck idle;

    @Before
    public void before() {
        context = MockOsgi.newBundleContext();
        idleCheck = new SubscriberIdleCheck(context, idle);
    }

    @Test
    public void testServiceRegistration() throws InterruptedException {
        ServiceReference<HealthCheck> ref = context.getServiceReference(HealthCheck.class);
        assertThat(ref, notNullValue());
        idleCheck.close();
        ServiceReference<HealthCheck> refClose = context.getServiceReference(HealthCheck.class);
        assertThat(refClose, nullValue());
    }

    @Test
    public void testName() throws InterruptedException {
        ServiceReference<HealthCheck> ref = context.getServiceReference(HealthCheck.class);
        assertThat(ref.getProperty(HealthCheck.NAME), equalTo(SubscriberIdleCheck.CHECK_NAME));
    }

    @Test
    public void testCheckCritical() throws InterruptedException {
        when(idle.isIdle()).thenReturn(false);
        verifyStatus(Result.Status.TEMPORARILY_UNAVAILABLE);
    }

    @Test
    public void testCheckOk() throws InterruptedException {
        when(idle.isIdle()).thenReturn(true);
        verifyStatus(Result.Status.OK);
    }

    private void verifyStatus(Result.Status expected) {
        Result result = idleCheck.execute();
        assertEquals(expected, result.getStatus());
    }
}
