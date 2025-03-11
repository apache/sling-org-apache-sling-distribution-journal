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

import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.vault.fs.api.ProgressTrackerListener.Mode.PATHS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.internal.MetricsServiceImpl;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;

public class ErrorListenerTest {

    private ErrorListener errorListener;

    private static final String path = "/some/path";

    private static final String errorMsg = "Failed to import XYZ";

    private static final Exception exception = new Exception(errorMsg);
    
    private OsgiContext context = new OsgiContext();

    @Before
    public void before() {
    	MetricsService metricsService = context.registerInjectActivateService(MetricsServiceImpl.class);
    	SubscriberMetrics subscriberMetrics = new SubscriberMetrics(metricsService, "publish_subscriber", "publish", false);
		errorListener = new ErrorListener(subscriberMetrics);
    }

    @Test
    public void testDefaultErrorIsNull() {
        assertNull(errorListener.getLastErrorMessage());
    }

    @Test
    public void testError() {
        errorListener.onError(PATHS, path, exception);
        String message = errorListener.getLastErrorMessage();
        assertNotNull(message);
        assertMessage(message);
    }

    @Test
    public void testLastError() {
        errorListener.onError(PATHS, path, new Exception());
        errorListener.onError(PATHS, path, exception);
        String message = errorListener.getLastErrorMessage();
        assertNotNull(message);
        assertMessage(message);
    }

    @Test
    public void testOnMessage() {
        errorListener.onMessage(PATHS, "action", path);
        errorListener.onError(PATHS, path, exception);
        String message = errorListener.getLastErrorMessage();
        assertNotNull(message);
        assertMessage(message);
    }

    private void assertMessage(String message) {
        assertTrue(message.contains(errorMsg));
        assertTrue(message.contains(path));
    }
}