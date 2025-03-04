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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.commons.metrics.Timer.Context;
import org.apache.sling.distribution.journal.bookkeeper.SubscriberMetrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SubscriberMetricsTest {
    
    SubscriberMetrics metrics;

    @Before
    public void before() {
        MetricsService metricsService = MetricsService.NOOP;
        metrics = new SubscriberMetrics(metricsService, "publishSubscriber", "publish", true);
    }

    public static void mockBehaviour(MetricsService metricsService) {
        when(metricsService.counter(Mockito.anyString())).thenReturn(Mockito.mock(Counter.class));
        Timer timer = Mockito.mock(Timer.class);
        when(timer.time()).thenReturn(Mockito.mock(Context.class));
        when(metricsService.timer(Mockito.anyString())).thenReturn(timer);
        when(metricsService.histogram(Mockito.anyString())).thenReturn(Mockito.mock(Histogram.class));
        when(metricsService.meter(Mockito.anyString())).thenReturn(Mockito.mock(Meter.class));
    }

    @Test
    public void testGetMetrics() {
        assertNotNull(metrics.getFailedPackageImports());
        assertNotNull(metrics.getImportedPackageDuration());
        assertNotNull(metrics.getImportedPackageSize());
        assertNotNull(metrics.getPackageDistributedDuration());
        assertNotNull(metrics.getPackageJournalDistributionDuration());
        assertNotNull(metrics.getRemovedFailedPackageDuration());
        assertNotNull(metrics.getRemovedPackageDuration());
        assertNotNull(metrics.getSendStoredStatusDuration());
        assertNotNull(metrics.getTransientImportErrors());
        assertNotNull(metrics.getPermanentImportErrors());
    }
}
