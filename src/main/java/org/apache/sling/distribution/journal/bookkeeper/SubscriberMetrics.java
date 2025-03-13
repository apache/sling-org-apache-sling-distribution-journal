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

import static org.apache.sling.distribution.journal.metrics.TaggedMetrics.getMetricName;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.metrics.Tag;

/**
 * Metrics for DistributionSubscriber
 * most metrics will have two parameters:
 * TAG_SUB_NAME and TAG_EDITABLE
 */
public class SubscriberMetrics {
    // Name of the subscriber agent
    private static final String TAG_SUB_NAME = "sub_name";
    
    // Name of the subscribed publish agent (first only if more than one)
    private static final String TAG_PUB_NAME = "pub_name";
    
    // Status of a package see org.apache.sling.distribution.journal.messages.PackageStatusMessage.Status
    private static final String TAG_STATUS = "status";
    
    // Is the queue editable (true, false)
    private static final String TAG_EDITABLE = "editable";
    
    public static final String SUB_COMPONENT = "distribution.journal.subscriber.";
    
    private static final String PACKAGE_STATUS_COUNT = SUB_COMPONENT + "package_status_count";
    
    // Number of packages with at least one failure to apply 
    private static final String CURRENT_RETRIES = SUB_COMPONENT + "current_retries";

    // Cumulated size of all packages (parameters: TAG_SUB_NAME, editable (golden publish))
    private static final String IMPORTED_PACKAGE_SIZE = SUB_COMPONENT + "imported_package_size";

    // Increased on every failure to apply a package
    private static final String FAILED_PACKAGE_IMPORTS = SUB_COMPONENT + "failed_package_imports";
    
    // Increased when a package failed before but then succeeded (parameters: agent, editable (golden publish))
    private static final String TRANSIENT_IMPORT_ERRORS = SUB_COMPONENT + "transient_import_errors";

    // Only counted in error queue setup
    private static final String PERMANENT_IMPORT_ERRORS = SUB_COMPONENT + "permanent_import_errors";
    
    // Counts every package that fails more than n times an thus causes a blocked queue
    private static final String BLOCKING_IMPORT_ERRORS = SUB_COMPONENT + "import_errors";

	
    private static final String IMPORT_PRE_PROCESS_REQUEST_COUNT = SUB_COMPONENT + "import_pre_process_request_count";
    private static final String IMPORT_POST_PROCESS_SUCCESS_COUNT = SUB_COMPONENT + "import_post_process_success_count";
    private static final String IMPORT_POST_PROCESS_REQUEST_COUNT = SUB_COMPONENT + "import_post_process_request_count";
    private static final String INVALIDATION_PROCESS_SUCCESS_COUNT = SUB_COMPONENT + "invalidation_process_success_count";
    private static final String INVALIDATION_PROCESS_REQUEST_COUNT = SUB_COMPONENT + "invalidation_process_request_count";
    private static final String IMPORT_PRE_PROCESS_SUCCESS_COUNT = SUB_COMPONENT + "import_pre_process_success_count";
	
    private static final String IMPORTED_PACKAGE_DURATION = SUB_COMPONENT + "imported_package_duration";
	private static final String IMPORT_PACKAGE_INSTALL_DURATION = SUB_COMPONENT + "package_install_duration";
	private static final String IMPORT_PACKAGE_INSTALL_COUNT = SUB_COMPONENT + "package_install_count";
    private static final String REMOVED_PACKAGE_DURATION = SUB_COMPONENT + "removed_package_duration";
    private static final String REMOVED_FAILED_PACKAGE_DURATION = SUB_COMPONENT + "removed_failed_package_duration";
    private static final String SEND_STORED_STATUS_DURATION = SUB_COMPONENT + "send_stored_status_duration";
    private static final String REQUEST_DISTRIBUTED_DURATION = SUB_COMPONENT + "request_distributed_duration";
    private static final String PACKAGE_JOURNAL_DISTRIBUTION_DURATION = SUB_COMPONENT + "package_journal_distribution_duration";
    private static final String IMPORT_PRE_PROCESS_DURATION = SUB_COMPONENT + "import_pre_process_duration";
    private static final String IMPORT_POST_PROCESS_DURATION = SUB_COMPONENT + "import_post_process_duration";
    private static final String INVALIDATION_PROCESS_DURATION = SUB_COMPONENT + "invalidation_process_duration";
    private static final String CURRENT_IMPORT_DURATION = SUB_COMPONENT + "current_import_duration";

	private static final String FV_MESSAGE_COUNT = SUB_COMPONENT + "fv_message_count";
	private static final String FV_ERROR_COUNT = SUB_COMPONENT + "fv_error_count";

    private final MetricsService metricsService;
    private final Tag tagSubName;
    private final Tag tagPubName;
    private final Tag tagEditable;
    private final List<Tag> tags;

    private final AtomicReference<CurrentImportInfo> currentImportInfo = new AtomicReference<CurrentImportInfo>();

    public SubscriberMetrics(MetricsService metricsService, String subAgentName, String pubAgentName, boolean editable) {
        this.metricsService = metricsService;
        tagSubName = Tag.of(TAG_SUB_NAME, subAgentName);
        tagEditable = Tag.of(TAG_EDITABLE, Boolean.toString(editable));
        tagPubName = Tag.of(TAG_PUB_NAME, pubAgentName);
        tags = Arrays.asList(
                tagSubName, 
                tagPubName,
                tagEditable);
        metricsService.gauge(getMetricName(CURRENT_IMPORT_DURATION, tags), this::getCurrentImportDuration);
    }

    /**
     * Histogram of the imported content package size in Byte.
     *
     * @return a Sling Metrics histogram
     */
    public Histogram getImportedPackageSize() {
        return metricsService.histogram(getMetricName(IMPORTED_PACKAGE_SIZE, tags));
    }

    /**
     * Timer capturing the duration in ms of successful packages import operations.
     *
     * @return a Sling Metrics timer
     */
    public Timer getImportedPackageDuration() {
        return metricsService.timer(getMetricName(IMPORTED_PACKAGE_DURATION, tags));
    }

    /**
     * Timer capturing the duration in ms of packages successfully removed from an editable subscriber.
     *
     * @return a Sling Metrics timer
     */
    public Timer getRemovedPackageDuration() {
        return metricsService.timer(getMetricName(REMOVED_PACKAGE_DURATION, tags));
    }

    /**
     * Timer capturing the duration in ms of packages successfully removed automatically from a subscriber supporting error queue.
     *
     * @return a Sling Metrics timer
     */
    public Timer getRemovedFailedPackageDuration() {
        return metricsService.timer(getMetricName(REMOVED_FAILED_PACKAGE_DURATION, tags));
    }

    /**
     * Meter of failures to import packages.
     *
     * @return a Sling Metrics meter
     */
    public Meter getFailedPackageImports() {
        return metricsService.meter(getMetricName(FAILED_PACKAGE_IMPORTS, tags));
    }

    /**
     * Timer capturing the duration in ms of sending a stored package status.
     *
     * @return a Sling Metric timer
     */
    public Timer getSendStoredStatusDuration() {
        return metricsService.timer(getMetricName(SEND_STORED_STATUS_DURATION, tags));
    }

    /**
     * Timer capturing the duration in ms of distributing a distribution package.
     * The timer starts when the package is enqueued and stops when the package is successfully imported.
     *
     * @return a Sling Metric timer
     */
    public Timer getPackageDistributedDuration() {
        return metricsService.timer(getMetricName(REQUEST_DISTRIBUTED_DURATION, tags));
    }

    /**
     * Timer capturing the duration in ms that a package spent in the distribution journal.
     * The timer starts when the package is enqueued and stops when the package is consumed.
     *
     * @return a Sling Metrics timer
     */
    public Timer getPackageJournalDistributionDuration() {
        return metricsService.timer(getMetricName(PACKAGE_JOURNAL_DISTRIBUTION_DURATION, tags));
    }

    /**
     * Counter for all the different package status.
     * @param pubAgentName name of publish agent
     * @param status after package import
     *
     * @return a Sling Metric counter
     */
    public Counter getPackageStatusCounter(String pubAgentName, Status status) {
        Tag tagPubName2 = Tag.of(TAG_PUB_NAME, pubAgentName);
        Tag tagStatus = Tag.of(TAG_STATUS, status.name());
        String name = getMetricName(PACKAGE_STATUS_COUNT, Arrays.asList(tagSubName, tagPubName2, tagEditable, tagStatus));
        return metricsService.counter(name);
    }

    public Timer getImportPreProcessDuration() {
        return metricsService.timer(getMetricName(IMPORT_PRE_PROCESS_DURATION, tags));
    }

    public Counter getImportPreProcessSuccess() {
        return metricsService.counter(getMetricName(IMPORT_PRE_PROCESS_SUCCESS_COUNT, tags));
    }

    public Counter getImportPreProcessRequest() {
        return metricsService.counter(getMetricName(IMPORT_PRE_PROCESS_REQUEST_COUNT, tags));
    }

    public Timer getImportPostProcessDuration() {
        return metricsService.timer(getMetricName(IMPORT_POST_PROCESS_DURATION, tags));
    }

    public Counter getImportPostProcessSuccess() {
        return metricsService.counter(getMetricName(IMPORT_POST_PROCESS_SUCCESS_COUNT, tags));
    }

    public Counter getImportPostProcessRequest() {
        return metricsService.counter(getMetricName(IMPORT_POST_PROCESS_REQUEST_COUNT, tags));
    }
    
    public Timer getPackgeInstallDuration() {
        return metricsService.timer(getMetricName(IMPORT_PACKAGE_INSTALL_DURATION, tags));
    }

    public Counter getPackageInstallCount() {
        return metricsService.counter(getMetricName(IMPORT_PACKAGE_INSTALL_COUNT, tags));
    }

    public Timer getInvalidationProcessDuration() {
        return metricsService.timer(getMetricName(INVALIDATION_PROCESS_DURATION, tags));
    }

    public Counter getInvalidationProcessSuccess() {
        return metricsService.counter(getMetricName(INVALIDATION_PROCESS_SUCCESS_COUNT, tags));
    }

    public Counter getInvalidationProcessRequest() {
        return metricsService.counter(getMetricName(INVALIDATION_PROCESS_REQUEST_COUNT, tags));
    }

    public Counter getTransientImportErrors() {
        return metricsService.counter(getMetricName(TRANSIENT_IMPORT_ERRORS, tags));
    }

    public Counter getPermanentImportErrors() { 
        return metricsService.counter(getMetricName(PERMANENT_IMPORT_ERRORS, tags));
    }
    
    public Counter getBlockingImportErrors() { 
        return metricsService.counter(getMetricName(BLOCKING_IMPORT_ERRORS, tags));
    }
    
    public Counter getFVMessages() { 
        return metricsService.counter(getMetricName(FV_MESSAGE_COUNT, tags));
    }
    
    public Counter getFVErrors() { 
        return metricsService.counter(getMetricName(FV_ERROR_COUNT, tags));
    }

    public void currentRetries(Supplier<Integer> retriesCallback) {
        metricsService.gauge(getMetricName(CURRENT_RETRIES, tags), retriesCallback);
    }
    
    public void setCurrentImport(CurrentImportInfo currentImport) {
    	this.currentImportInfo.set(currentImport);
    }
    
    public void clearCurrentImport() {
    	this.currentImportInfo.set(null);
    }
    
	public long getCurrentImportDuration() {
		CurrentImportInfo importInfo = currentImportInfo.get();
		return importInfo == null ? 0L : importInfo.getCurrentImportDuration();
	}
}
 