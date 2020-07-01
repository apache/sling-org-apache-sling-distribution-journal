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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Gauge;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.commons.metrics.Timer.Context;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService.GaugeService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

@RunWith(MockitoJUnitRunner.class)
public class DistributionMetricsServiceTest {
    
    @Mock
    MetricsService metricsService;
    
    @InjectMocks
    DistributionMetricsService metrics;

    @Mock
    private BundleContext context;

    @SuppressWarnings("rawtypes")
    @Mock
    private ServiceRegistration<Gauge> reg;

    @Mock
    private Timer timer;

    @Mock
    private Histogram histogram;

    @Mock
    private Meter meter;
    
    @Before
    public void before() {
        mockBehaviour(metricsService);
        
        when(context.registerService(Mockito.eq(Gauge.class), Mockito.any(Gauge.class), Mockito.any())).thenReturn(reg);
        metrics.activate(context);
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
        assertNotNull(metrics.getAcceptedRequests());
        assertNotNull(metrics.getBuildPackageDuration());
        assertNotNull(metrics.getCleanupPackageDuration());
        assertNotNull(metrics.getCleanupPackageRemovedCount());
        assertNotNull(metrics.getDroppedRequests());
        assertNotNull(metrics.getEnqueuePackageDuration());
        assertNotNull(metrics.getExportedPackageSize());
        assertNotNull(metrics.getFailedPackageImports());
        assertNotNull(metrics.getImportedPackageDuration());
        assertNotNull(metrics.getImportedPackageSize());
        assertNotNull(metrics.getItemsBufferSize());
        assertNotNull(metrics.getPackageDistributedDuration());
        assertNotNull(metrics.getProcessQueueItemDuration());
        assertNotNull(metrics.getQueueCacheFetchCount());
        assertNotNull(metrics.getQueueAccessErrorCount());
        assertNotNull(metrics.getRemovedFailedPackageDuration());
        assertNotNull(metrics.getRemovedPackageDuration());
        assertNotNull(metrics.getSendStoredStatusDuration());
    }
    
    @Test
    public void testGauge() {
        GaugeService<Integer> gauge = metrics.createGauge("name", "desc", () -> 42);
        verify(context).registerService(Mockito.eq(Gauge.class), Mockito.eq(gauge), Mockito.any());
        assertThat(gauge.getValue(), equalTo(42));
        gauge.close();
        verify(reg).unregister();
    }
    
    @Test
    public void testGagueErrorOnClose() {
        doThrow(new IllegalStateException("Expected")).when(reg).unregister();
        GaugeService<Integer> gauge = metrics.createGauge("name", "desc", () -> 42);
        assertThat(gauge.getValue(), equalTo(42));
        gauge.close();
        verify(reg).unregister();
    }
}
