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

import static java.lang.String.format;

import java.util.function.Supplier;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(service = SubscriberMetrics.class)
public class SubscriberMetrics {
    public static final String SUB_COMPONENT = "distribution.journal.subscriber";
    
    private final MetricsService metricsService;

    private final Histogram importedPackageSize;

    private final  Counter itemsBufferSize;

    private final  Timer removedPackageDuration;

    private final  Timer removedFailedPackageDuration;

    private final  Timer importedPackageDuration;

    private final  Meter failedPackageImports;

    private final  Timer sendStoredStatusDuration;

    private final  Timer processQueueItemDuration;

    private final  Timer packageDistributedDuration;

    private final  Timer packageJournalDistributionDuration;

    private final  Timer importPostProcessDuration;
    
    private final  Counter importPostProcessSuccess;

    private final  Counter importPostProcessRequest;

    private final  Timer invalidationProcessDuration;

    private final  Counter invalidationProcessSuccess;

    private final  Counter invalidationProcessRequest;

    private final  Counter transientImportErrors;

    private final  Counter permanentImportErrors;

    @Activate
    public SubscriberMetrics(@Reference MetricsService metricsService) {
        this.metricsService = metricsService;
        importedPackageSize = metricsService.histogram(getMetricName("imported_package_size"));
        itemsBufferSize = metricsService.counter(getMetricName("items_buffer_size"));
        importedPackageDuration = metricsService.timer(getMetricName("imported_package_duration"));
        removedPackageDuration = metricsService.timer(getMetricName("removed_package_duration"));
        removedFailedPackageDuration = metricsService.timer(getMetricName("removed_failed_package_duration"));
        failedPackageImports = metricsService.meter(getMetricName("failed_package_imports"));
        sendStoredStatusDuration = metricsService.timer(getMetricName("send_stored_status_duration"));
        processQueueItemDuration = metricsService.timer(getMetricName("process_queue_item_duration"));
        packageDistributedDuration = metricsService.timer(getMetricName("request_distributed_duration"));
        packageJournalDistributionDuration = metricsService.timer(getMetricName("package_journal_distribution_duration"));
        importPostProcessDuration = metricsService.timer(getMetricName("import_post_process_duration"));
        importPostProcessSuccess = metricsService.counter(getMetricName("import_post_process_success_count"));
        importPostProcessRequest = metricsService.counter(getMetricName("import_post_process_request_count"));
        invalidationProcessDuration = metricsService.timer(getMetricName("invalidation_process_duration"));
        invalidationProcessSuccess = metricsService.counter(getMetricName("invalidation_process_success_count"));
        invalidationProcessRequest = metricsService.counter(getMetricName("invalidation_process_request_count"));
        transientImportErrors = metricsService.counter(getMetricName("transient_import_errors"));
        permanentImportErrors = metricsService.counter(getMetricName("permanent_import_errors"));
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
     * Counter for all the different package status.
     *
     * @return a Sling Metric counter
     */
    public Counter getPackageStatusCounter(Status status) {
        return metricsService.counter(getNameWithLabel(getMetricName("package_status_count"), "status", status.name()));
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

    public void currentRetries(String subAgentName, Supplier<Integer> retriesCallback) {
        String nameRetries = SubscriberMetrics.SUB_COMPONENT + ".current_retries;sub_name=" + subAgentName;
        metricsService.gauge(nameRetries, retriesCallback);
    }
    
    private String getMetricName(String name) {
        return format("%s.%s", SUB_COMPONENT, name);
    }

    private String getNameWithLabel(String name, String label, String labelVal) {
        return format("%s;%s=%s", name, label, labelVal);
    }
}
