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

import java.io.Closeable;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.commons.metrics.Gauge;
import org.apache.sling.commons.metrics.Histogram;
import org.apache.sling.commons.metrics.Meter;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.Timer;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(service = DistributionMetricsService.class)
public class DistributionMetricsService {

    public static final String BASE_COMPONENT = "distribution.journal";

    public static final String PUB_COMPONENT = BASE_COMPONENT + ".publisher";

    public static final String SUB_COMPONENT = BASE_COMPONENT + ".subscriber";
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Reference
    private MetricsService metricsService;

    private Counter cleanupPackageRemovedCount;

    private Timer cleanupPackageDuration;

    private Histogram importedPackageSize;

    private Histogram exportedPackageSize;

    private Meter acceptedRequests;

    private Meter droppedRequests;

    private Counter itemsBufferSize;

    private Timer removedPackageDuration;

    private Timer removedFailedPackageDuration;

    private Timer importedPackageDuration;

    private Meter failedPackageImports;

    private Timer sendStoredStatusDuration;

    private Timer processQueueItemDuration;

    private Timer packageDistributedDuration;

    private Timer buildPackageDuration;

    private Timer enqueuePackageDuration;

    private Counter queueCacheFetchCount;

    private Counter queueAccessErrorCount;

    private BundleContext context;

    @Activate
    public void activate(BundleContext context) {
        this.context = context;
        cleanupPackageRemovedCount = getCounter(getMetricName(PUB_COMPONENT, "cleanup_package_removed_count"));
        cleanupPackageDuration = getTimer(getMetricName(PUB_COMPONENT, "cleanup_package_duration"));
        exportedPackageSize = getHistogram(getMetricName(PUB_COMPONENT, "exported_package_size"));
        acceptedRequests = getMeter(getMetricName(PUB_COMPONENT, "accepted_requests"));
        droppedRequests = getMeter(getMetricName(PUB_COMPONENT, "dropped_requests"));
        buildPackageDuration = getTimer(getMetricName(PUB_COMPONENT, "build_package_duration"));
        enqueuePackageDuration = getTimer(getMetricName(PUB_COMPONENT, "enqueue_package_duration"));
        queueCacheFetchCount = getCounter(getMetricName(PUB_COMPONENT, "queue_cache_fetch_count"));

        importedPackageSize = getHistogram(getMetricName(SUB_COMPONENT, "imported_package_size"));
        itemsBufferSize = getCounter(getMetricName(SUB_COMPONENT, "items_buffer_size"));
        importedPackageDuration = getTimer(getMetricName(SUB_COMPONENT, "imported_package_duration"));
        removedPackageDuration = getTimer(getMetricName(SUB_COMPONENT, "removed_package_duration"));
        removedFailedPackageDuration = getTimer(getMetricName(SUB_COMPONENT, "removed_failed_package_duration"));
        failedPackageImports = getMeter(getMetricName(SUB_COMPONENT, "failed_package_imports"));
        sendStoredStatusDuration = getTimer(getMetricName(SUB_COMPONENT, "send_stored_status_duration"));
        processQueueItemDuration = getTimer(getMetricName(SUB_COMPONENT, "process_queue_item_duration"));
        packageDistributedDuration = getTimer(getMetricName(SUB_COMPONENT, "request_distributed_duration"));
        queueAccessErrorCount = getCounter(getMetricName(PUB_COMPONENT, "queue_access_error_count"));
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
    
    public <T> GaugeService<T> createGauge(String name, String description, Supplier<T> supplier) {
        return new GaugeService<>(name, description, supplier);
    }

    private String getMetricName(String component, String name) {
        return format("%s.%s", component, name);
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

    public class GaugeService<T> implements Gauge<T>, Closeable {
        
        @SuppressWarnings("rawtypes")
        private final ServiceRegistration<Gauge> reg;
        private final Supplier<T> supplier;

        private GaugeService(String name, String description, Supplier<T> supplier) {
            this.supplier = supplier;
            Dictionary<String, String> props = new Hashtable<>();
            props.put(Constants.SERVICE_DESCRIPTION, description);
            props.put(Constants.SERVICE_VENDOR, "The Apache Software Foundation");
            props.put(Gauge.NAME, name);
            reg = context.registerService(Gauge.class, this, props);
        }

        @Override
        public T getValue() {
            return supplier.get();
        }
        
        @Override
        public void close() {
            try {
                reg.unregister();
            } catch (Exception e) {
                log.warn("Error unregistering service", e);
            }
        }
    }
    
}
