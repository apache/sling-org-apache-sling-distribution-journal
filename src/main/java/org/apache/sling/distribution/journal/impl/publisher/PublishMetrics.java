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

import static java.lang.String.format;

import java.util.function.Supplier;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.Timer;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(service = PublishMetrics.class)
public class PublishMetrics {

    public static final String BASE_COMPONENT = "distribution.journal";

    public static final String PUB_COMPONENT = BASE_COMPONENT + ".publisher";

    private final MetricsService metricsService;

    private final  Histogram exportedPackageSize;

    private final  Meter acceptedRequests;

    private final  Meter droppedRequests;

    private final  Timer buildPackageDuration;

    private final  Timer enqueuePackageDuration;

    private final  Counter queueCacheFetchCount;

    private final  Counter queueAccessErrorCount;

    @Activate
    public PublishMetrics(@Reference MetricsService metricsService) {
        this.metricsService = metricsService;
        exportedPackageSize = metricsService.histogram(getMetricName("exported_package_size"));
        acceptedRequests = metricsService.meter(getMetricName("accepted_requests"));
        droppedRequests = metricsService.meter(getMetricName("dropped_requests"));
        buildPackageDuration = metricsService.timer(getMetricName("build_package_duration"));
        enqueuePackageDuration = metricsService.timer(getMetricName("enqueue_package_duration"));
        queueCacheFetchCount = metricsService.counter(getMetricName("queue_cache_fetch_count"));
        queueAccessErrorCount = metricsService.counter(getMetricName("queue_access_error_count"));
    }

    /**
     * Histogram of the exported content package size in Bytes.
     *
     * @return a Sling Metrics histogram
     */
    public Histogram getExportedPackageSize() {
        return exportedPackageSize;
    }

    /**
     * Meter of requests returning an {@code DistributionRequestState.ACCEPTED} state.
     *
     * @return a Sling Metrics meter
     */
    public Meter getAcceptedRequests() {
        return acceptedRequests;
    }

    /**
     * Meter of requests returning an {@code DistributionRequestState.DROPPED} state.
     *
     * @return a Sling Metrics meter
     */
    public Meter getDroppedRequests() {
        return droppedRequests;
    }

    /**
     * Timer capturing the duration in ms of building a content package
     *
     * @return a Sling Metric timer
     */
    public Timer getBuildPackageDuration() {
        return buildPackageDuration;
    }

    /**
     * Timer capturing the duration in ms of adding a package to the queue
     *
     * @return a Sling Metric timer
     */
    public Timer getEnqueuePackageDuration() {
        return enqueuePackageDuration;
    }

    /**
     * Counter of fetch operations to feed the queue cache.
     *
     * @return a Sling Metric counter
     */
    public Counter getQueueCacheFetchCount() {
        return queueCacheFetchCount;
    }

    /**
     * Counter of queue access errors.
     *
     * @return a Sling Metric counter
     */
    public Counter getQueueAccessErrorCount() {
        return queueAccessErrorCount;
    }

    public void subscriberCount(String pubAgentName, Supplier<Integer> subscriberCountCallback) {
        metricsService.gauge(PublishMetrics.PUB_COMPONENT + ".subscriber_count;pub_name=" + pubAgentName,
                subscriberCountCallback);
        
    }

    private String getMetricName(String name) {
        return format("%s.%s", PUB_COMPONENT, name);
    }

}
