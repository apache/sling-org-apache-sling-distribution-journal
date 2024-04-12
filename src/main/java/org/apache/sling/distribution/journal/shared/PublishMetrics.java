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

import static java.lang.String.format;

import java.util.function.Supplier;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Gauge;
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
        exportedPackageSize = getHistogram(getMetricName(PUB_COMPONENT, "exported_package_size"));
        acceptedRequests = getMeter(getMetricName(PUB_COMPONENT, "accepted_requests"));
        droppedRequests = getMeter(getMetricName(PUB_COMPONENT, "dropped_requests"));
        buildPackageDuration = getTimer(getMetricName(PUB_COMPONENT, "build_package_duration"));
        enqueuePackageDuration = getTimer(getMetricName(PUB_COMPONENT, "enqueue_package_duration"));
        queueCacheFetchCount = getCounter(getMetricName(PUB_COMPONENT, "queue_cache_fetch_count"));
        queueAccessErrorCount = getCounter(getMetricName(PUB_COMPONENT, "queue_access_error_count"));
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

    /**
     * Counter of journal error codes.
     *
     * @return a Sling Metric counter
     */
    public Counter getJournalErrorCodeCount(String errorCode) {
        return getCounter(
            getNameWithLabel(getMetricName(BASE_COMPONENT, "journal_unavailable_error_code_count"), "error_code", errorCode));
    }

    /**
     * Counter for all the different package status.
     *
     * @return a Sling Metric counter
     */
    public Counter getPackageStatusCounter(String status) {
        return getCounter(
                getNameWithLabel(getMetricName(BASE_COMPONENT, "package_status_count"), "status", status)
        );
    }
    
    public void subscriberCount(String pubAgentName, Supplier<Integer> subscriberCountCallback) {
        createGauge(PublishMetrics.PUB_COMPONENT + ".subscriber_count;pub_name=" + pubAgentName,
                subscriberCountCallback);
        
    }

    private <T> Gauge<T> createGauge(String name, Supplier<T> supplier) {
        return metricsService.gauge(name, supplier);
    }

    private String getMetricName(String component, String name) {
        return format("%s.%s", component, name);
    }

    private String getNameWithLabel(String name, String label, String labelVal) {
        return format("%s;%s=%s", name, label, labelVal);
    }

    private Counter getCounter(String metricName) {
        return metricsService.counter(metricName);
    }

    private Timer getTimer(String metricName) {
        return metricsService.timer(metricName);
    }

    private Histogram getHistogram(String metricName) {
        return metricsService.histogram(metricName);
    }

    private Meter getMeter(String metricName) {
        return metricsService.meter(metricName);
    }

}
