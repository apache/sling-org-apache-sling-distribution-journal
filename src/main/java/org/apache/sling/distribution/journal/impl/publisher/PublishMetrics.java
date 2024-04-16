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

import static org.apache.sling.distribution.journal.metrics.TaggedMetrics.getMetricName;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.journal.metrics.Tag;

public class PublishMetrics {
    private static final String TAG_AGENT_NAME = "pub_name";

    public static final String PUB_COMPONENT = "distribution.journal.publisher.";
    private static final String EXPORTED_PACKAGE_SIZE = PUB_COMPONENT + "exported_package_size";
    private static final String ACCEPTED_REQUESTS = PUB_COMPONENT + "accepted_requests";
    private static final String DROPPED_REQUESTS = PUB_COMPONENT + "dropped_requests";
    private static final String BUILD_PACKAGE_DURATION = PUB_COMPONENT + "build_package_duration";
    private static final String ENQUEUE_PACKAGE_DURATION = PUB_COMPONENT + "enqueue_package_duration";
    private static final String QUEUE_CACHE_FETCH_COUNT = PUB_COMPONENT + "queue_cache_fetch_count";
    private static final String QUEUE_ACCESS_ERROR_COUNT = PUB_COMPONENT + "queue_access_error_count";
    private static final String SUBSCRIBER_COUNT = PUB_COMPONENT + "subscriber_count";

    private final List<Tag> tags;
    private final MetricsService metricsService;

    public PublishMetrics(MetricsService metricsService, String pubAgentName) {
        this.tags = Arrays.asList(Tag.of(TAG_AGENT_NAME, pubAgentName));
        this.metricsService = metricsService;
    }

    /**
     * Histogram of the exported content package size in Bytes.
     *
     * @return a Sling Metrics histogram
     */
    public Histogram getExportedPackageSize() {
        return metricsService.histogram(getMetricName(EXPORTED_PACKAGE_SIZE, tags));
    }

    /**
     * Meter of requests returning an {@code DistributionRequestState.ACCEPTED} state.
     *
     * @return a Sling Metrics meter
     */
    public Meter getAcceptedRequests() {
        return metricsService.meter(getMetricName(ACCEPTED_REQUESTS, tags));
    }

    /**
     * Meter of requests returning an {@code DistributionRequestState.DROPPED} state.
     *
     * @return a Sling Metrics meter
     */
    public Meter getDroppedRequests() {
        return metricsService.meter(getMetricName(DROPPED_REQUESTS, tags));
    }

    /**
     * Timer capturing the duration in ms of building a content package
     *
     * @return a Sling Metric timer
     */
    public Timer getBuildPackageDuration() {
        return metricsService.timer(getMetricName(BUILD_PACKAGE_DURATION, tags));
    }

    /**
     * Timer capturing the duration in ms of adding a package to the queue
     *
     * @return a Sling Metric timer
     */
    public Timer getEnqueuePackageDuration() {
        return metricsService.timer(getMetricName(ENQUEUE_PACKAGE_DURATION, tags));
    }

    /**
     * Counter of fetch operations to feed the queue cache.
     *
     * @return a Sling Metric counter
     */
    public Counter getQueueCacheFetchCount() {
        return metricsService.counter(getMetricName(QUEUE_CACHE_FETCH_COUNT, tags));
    }

    /**
     * Counter of queue access errors.
     *
     * @return a Sling Metric counter
     */
    public Counter getQueueAccessErrorCount() {
        return metricsService.counter(getMetricName(QUEUE_ACCESS_ERROR_COUNT, tags));
    }

    public void subscriberCount(Supplier<Integer> subscriberCountCallback) {
        metricsService.gauge(getMetricName(SUBSCRIBER_COUNT, tags), subscriberCountCallback);
    }

}
