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

import java.util.concurrent.Callable;
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

@Component(service = DistributionMetricsService.class)
public class DistributionMetricsService {

    public static final String BASE_COMPONENT = "distribution.journal";

    public static final String PUB_COMPONENT = BASE_COMPONENT + ".publisher";

    public static final String SUB_COMPONENT = BASE_COMPONENT + ".subscriber";
    
    private final MetricsService metricsService;

    private final Counter cleanupPackageRemovedCount;

    private final Timer cleanupPackageDuration;

    private final Histogram importedPackageSize;

    private final  Histogram exportedPackageSize;

    private final  Meter acceptedRequests;

    private final  Meter droppedRequests;

    private final  Counter itemsBufferSize;

    private final  Timer removedPackageDuration;

    private final  Timer removedFailedPackageDuration;

    private final  Timer importedPackageDuration;

    private final  Meter failedPackageImports;

    private final  Timer sendStoredStatusDuration;

    private final  Timer processQueueItemDuration;

    private final  Timer packageDistributedDuration;

    private final  Timer packageJournalDistributionDuration;

    private final  Timer buildPackageDuration;

    private final  Timer enqueuePackageDuration;

    private final  Counter queueCacheFetchCount;

    private final  Counter queueAccessErrorCount;

    private final  Timer importPostProcessDuration;
    
    private final  Counter importPostProcessSuccess;

    private final  Counter importPostProcessRequest;

    private final  Timer invalidationProcessDuration;

    private final  Counter invalidationProcessSuccess;

    private final  Counter invalidationProcessRequest;

    private final  Counter transientImportErrors;

    private final  Counter permanentImportErrors;

    private final Counter queueSizeLimitReached;

    @Activate
    public DistributionMetricsService(@Reference MetricsService metricsService) {
        this.metricsService = metricsService;
        cleanupPackageRemovedCount = getCounter(getMetricName(PUB_COMPONENT, "cleanup_package_removed_count"));
        cleanupPackageDuration = getTimer(getMetricName(PUB_COMPONENT, "cleanup_package_duration"));
        exportedPackageSize = getHistogram(getMetricName(PUB_COMPONENT, "exported_package_size"));
        acceptedRequests = getMeter(getMetricName(PUB_COMPONENT, "accepted_requests"));
        droppedRequests = getMeter(getMetricName(PUB_COMPONENT, "dropped_requests"));
        buildPackageDuration = getTimer(getMetricName(PUB_COMPONENT, "build_package_duration"));
        enqueuePackageDuration = getTimer(getMetricName(PUB_COMPONENT, "enqueue_package_duration"));
        queueCacheFetchCount = getCounter(getMetricName(PUB_COMPONENT, "queue_cache_fetch_count"));
        queueSizeLimitReached = getCounter(getMetricName(PUB_COMPONENT, "queue_size_limit_reached"));
        importedPackageSize = getHistogram(getMetricName(SUB_COMPONENT, "imported_package_size"));
        itemsBufferSize = getCounter(getMetricName(SUB_COMPONENT, "items_buffer_size"));
        importedPackageDuration = getTimer(getMetricName(SUB_COMPONENT, "imported_package_duration"));
        removedPackageDuration = getTimer(getMetricName(SUB_COMPONENT, "removed_package_duration"));
        removedFailedPackageDuration = getTimer(getMetricName(SUB_COMPONENT, "removed_failed_package_duration"));
        failedPackageImports = getMeter(getMetricName(SUB_COMPONENT, "failed_package_imports"));
        sendStoredStatusDuration = getTimer(getMetricName(SUB_COMPONENT, "send_stored_status_duration"));
        processQueueItemDuration = getTimer(getMetricName(SUB_COMPONENT, "process_queue_item_duration"));
        packageDistributedDuration = getTimer(getMetricName(SUB_COMPONENT, "request_distributed_duration"));
        packageJournalDistributionDuration = getTimer(getMetricName(SUB_COMPONENT, "package_journal_distribution_duration"));
        queueAccessErrorCount = getCounter(getMetricName(PUB_COMPONENT, "queue_access_error_count"));
        importPostProcessDuration = getTimer(getMetricName(PUB_COMPONENT, "import_post_process_duration"));
        importPostProcessSuccess = getCounter(getMetricName(SUB_COMPONENT, "import_post_process_success_count"));
        importPostProcessRequest = getCounter(getMetricName(SUB_COMPONENT, "import_post_process_request_count"));
        invalidationProcessDuration = getTimer(getMetricName(PUB_COMPONENT, "invalidation_process_duration"));
        invalidationProcessSuccess = getCounter(getMetricName(SUB_COMPONENT, "invalidation_process_success_count"));
        invalidationProcessRequest = getCounter(getMetricName(SUB_COMPONENT, "invalidation_process_request_count"));
        transientImportErrors = getCounter(getMetricName(SUB_COMPONENT, "transient_import_errors"));
        permanentImportErrors = getCounter(getMetricName(SUB_COMPONENT, "permanent_import_errors"));
    }

    /**
     * Runs provided code updating provided metric
     * with its execution time.
     * The method guarantees that the metric is updated
     * even if the code throws an exception
     * @param metric metric to update
     * @param code code to clock
     * @throws Exception actually it doesn't
     */
    public static void timed(Timer metric, Runnable code) throws Exception {
        try (Timer.Context ignored = metric.time()) {
            code.run();
        }
    }

    /**
     * Runs provided code updating provided metric
     * with its execution time.
     * The method guarantees that the metric is updated
     * even if the code throws an exception
     * @param metric metric to update
     * @param code code to clock
     * @return a value returned but <code>code.call()</code> invocation
     * @throws Exception if underlying code throws
     */
    public static <T> T timed(Timer metric, Callable<T> code) throws Exception {
        try (Timer.Context ignored = metric.time()) {
            return code.call();
        }
    }

    /**
     * Counter of package removed during the Package Cleanup Task.
     * The count is the sum of all packages removed since the service started.
     *
     * @return a Sling Metrics timer
     */
    public Counter getCleanupPackageRemovedCount() {
        return cleanupPackageRemovedCount;
    }

    /**
     * Timer of the Package Cleanup Task execution duration.
     *
     * @return a Sling Metrics timer
     */
    public Timer getCleanupPackageDuration() {
        return cleanupPackageDuration;
    }

    /**
     * Histogram of the imported content package size in Byte.
     *
     * @return a Sling Metrics histogram
     */
    public Histogram getImportedPackageSize() {
        return importedPackageSize;
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
     * Counter of the package buffer size on the subscriber.
     *
     * @return a Sling Metrics counter
     */
    public Counter getItemsBufferSize() {
        return itemsBufferSize;
    } 

    /**
     * Timer capturing the duration in ms of successful packages import operations.
     *
     * @return a Sling Metrics timer
     */
    public Timer getImportedPackageDuration() {
        return importedPackageDuration;
    }

    /**
     * Timer capturing the duration in ms of packages successfully removed from an editable subscriber.
     *
     * @return a Sling Metrics timer
     */
    public Timer getRemovedPackageDuration() {
        return removedPackageDuration;
    }

    /**
     * Timer capturing the duration in ms of packages successfully removed automatically from a subscriber supporting error queue.
     *
     * @return a Sling Metrics timer
     */
    public Timer getRemovedFailedPackageDuration() {
        return removedFailedPackageDuration;
    }

    /**
     * Meter of failures to import packages.
     *
     * @return a Sling Metrics meter
     */
    public Meter getFailedPackageImports() {
        return failedPackageImports;
    }

    /**
     * Timer capturing the duration in ms of sending a stored package status.
     *
     * @return a Sling Metric timer
     */
    public Timer getSendStoredStatusDuration() {
        return sendStoredStatusDuration;
    }

    /**
     * Timer capturing the duration in ms of processing a queue item.
     *
     * @return a Sling Metric timer
     */
    public Timer getProcessQueueItemDuration() {
        return processQueueItemDuration;
    }

    /**
     * Timer capturing the duration in ms of distributing a distribution package.
     * The timer starts when the package is enqueued and stops when the package is successfully imported.
     *
     * @return a Sling Metric timer
     */
    public Timer getPackageDistributedDuration() {
        return packageDistributedDuration;
    }

    /**
     * Timer capturing the duration in ms that a package spent in the distribution journal.
     * The timer starts when the package is enqueued and stops when the package is consumed.
     *
     * @return a Sling Metrics timer
     */
    public Timer getPackageJournalDistributionDuration() {
        return packageJournalDistributionDuration;
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

    public <T> Gauge<T> createGauge(String name, Supplier<T> supplier) {
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

    public Timer getImportPostProcessDuration() {
        return importPostProcessDuration;
    }

    public Counter getImportPostProcessSuccess() {
        return importPostProcessSuccess;
    }

    public Counter getImportPostProcessRequest() {
        return importPostProcessRequest;
    }

    public Timer getInvalidationProcessDuration() {
        return invalidationProcessDuration;
    }

    public Counter getInvalidationProcessSuccess() {
        return invalidationProcessSuccess;
    }

    public Counter getInvalidationProcessRequest() {
        return invalidationProcessRequest;
    }

    public Counter getTransientImportErrors() {
        return transientImportErrors;
    }

    public Counter getPermanentImportErrors() { 
        return permanentImportErrors;
    }

    public Counter getQueueSizeLimitReached() {
        return queueSizeLimitReached;
    }

}
