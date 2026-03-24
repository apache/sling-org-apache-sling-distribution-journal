# Metrics Overview

This document provides a comprehensive overview of all metrics in the Apache Sling Distribution Journal bundle, organized by publisher and subscriber components.

## Publisher Metrics

Most publisher metrics are prefixed with `sling_distribution_journal_publisher_` and include:
- `pub_name`: Name of the publish agent

The `queue_size` gauge additionally includes `clearable` (see below).

### Package Export Metrics

#### `sling_distribution_journal_publisher_exported_package_size` (Histogram)
- **Type**: Histogram
- **Unit**: Bytes
- **Description**: Histogram of the exported content package size in bytes
- **Tags**: `pub_name`

### Request Metrics

#### `sling_distribution_journal_publisher_accepted_requests` (Meter)
- **Type**: Meter
- **Description**: Rate of requests returning a `DistributionRequestState.ACCEPTED` state
- **Tags**: `pub_name`

#### `sling_distribution_journal_publisher_dropped_requests` (Meter)
- **Type**: Meter
- **Description**: Rate of requests returning a `DistributionRequestState.DROPPED` state
- **Tags**: `pub_name`

### Package Building Metrics

#### `sling_distribution_journal_publisher_build_package_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of building a content package
- **Tags**: `pub_name`

#### `sling_distribution_journal_publisher_enqueue_package_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of adding a package to the queue
- **Tags**: `pub_name`

### Queue Metrics

#### `sling_distribution_journal_publisher_queue_size` (Gauge)
- **Type**: Gauge
- **Description**: Max backlog depth for a subscriber cohort, measured from the minimum `lastProcessedOffset` in that cohort (tail size in the journal cache). **Clearable** cohort: subscribers with a non-null clear callback on `QueueState`. **Non-clearable** cohort: subscribers with a `QueueState` and a null clear callback. Two time series per publisher (see tags). Values are cached and refreshed in the background every 30 seconds. Publisher throttling uses the **clearable** series only (`clearable=true`).
- **Tags**: `pub_name`, `clearable` (`true` or `false`)
- **Migration**: Previously this metric used only `pub_name`. Series are now distinguished by `clearable`; update dashboards and alerts to include the `clearable` tag (e.g. `clearable=true` for the former single series).
- **Staleness**: The value can be up to ~30 seconds stale under normal conditions. When queue size computation scales linearly (O(n)) with queue size and takes longer than the refresh interval, staleness increases: refresh cycles can back up, and the displayed value may lag further behind the true queue size. See `sling_distribution_journal_publisher_queue_size_computation_duration` for monitoring computation cost.

#### `sling_distribution_journal_publisher_queue_size_computation_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of computing **both** clearable and non-clearable max queue sizes for an agent during each background refresh. The computation is O(n) with queue size; slow values (>30s) indicate increased staleness.
- **Tags**: `pub_name`

#### `sling_distribution_journal_publisher_queue_cache_fetch_count` (Counter)
- **Type**: Counter
- **Description**: Count of fetch operations to feed the queue cache
- **Tags**: `pub_name`

#### `sling_distribution_journal_publisher_queue_access_error_count` (Counter)
- **Type**: Counter
- **Description**: Count of queue access errors
- **Tags**: `pub_name`

### Subscriber Discovery Metrics

#### `sling_distribution_journal_publisher_subscriber_count` (Gauge)
- **Type**: Gauge
- **Description**: Current number of subscribers for this publisher
- **Tags**: `pub_name`

---

## Subscriber Metrics

All subscriber metrics are prefixed with `sling_distribution_journal_subscriber_` and typically include the following tags:
- `sub_name`: Name of the subscriber agent
- `pub_name`: Name of the subscribed publish agent (first only if more than one)
- `editable`: Whether the queue is editable (true/false)

### Package Import Metrics

#### `sling_distribution_journal_subscriber_imported_package_size` (Histogram)
- **Type**: Histogram
- **Unit**: Bytes
- **Description**: Histogram of the imported content package size in bytes
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_imported_package_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of successful package import operations
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_current_import_duration` (Gauge)
- **Type**: Gauge
- **Unit**: Milliseconds
- **Description**: Current duration of the package import operation in progress (0 if no import is currently happening)
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_failed_package_imports` (Meter)
- **Type**: Meter
- **Description**: Rate of failures to import packages (increased on every failure to apply a package)
- **Tags**: `sub_name`, `pub_name`, `editable`

### Package Removal Metrics

#### `sling_distribution_journal_subscriber_removed_package_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of packages successfully removed from an editable subscriber
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_removed_failed_package_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of packages successfully removed automatically from a subscriber supporting error queue
- **Tags**: `sub_name`, `pub_name`, `editable`

### Package Status Metrics

#### `sling_distribution_journal_subscriber_package_status_count` (Counter)
- **Type**: Counter
- **Description**: Counter for all the different package statuses. Tracks the number of packages that reached each status
- **Tags**: `sub_name`, `pub_name`, `editable`, `status`
- **Status Values**:
  - `IMPORTED`: Package was successfully imported
  - `REMOVED`: Package was successfully removed from an editable subscriber
  - `REMOVED_FAILED`: Package failed and was removed from error queue

### Error Metrics

#### `sling_distribution_journal_subscriber_transient_import_errors` (Counter)
- **Type**: Counter
- **Description**: Count of packages that failed before but then succeeded (transient errors that were eventually resolved)
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_permanent_import_errors` (Counter)
- **Type**: Counter
- **Description**: Count of permanent import errors (only counted in error queue setup)
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_import_errors` (Counter)
- **Type**: Counter
- **Description**: Count of packages that failed more than n times and thus caused a blocked queue (blocking import errors)
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_current_retries` (Gauge)
- **Type**: Gauge
- **Description**: Number of packages with at least one failure to apply (current retry count)
- **Tags**: `sub_name`, `pub_name`, `editable`

### Processing Metrics

#### `sling_distribution_journal_subscriber_import_pre_process_request_count` (Counter)
- **Type**: Counter
- **Description**: Number of import pre-processing requests
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_import_pre_process_success_count` (Counter)
- **Type**: Counter
- **Description**: Number of successful import pre-processing operations
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_import_pre_process_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of import pre-processing operations
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_import_post_process_request_count` (Counter)
- **Type**: Counter
- **Description**: Number of import post-processing requests
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_import_post_process_success_count` (Counter)
- **Type**: Counter
- **Description**: Number of successful import post-processing operations
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_import_post_process_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of import post-processing operations
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_package_install_count` (Counter)
- **Type**: Counter
- **Description**: Number of package installation operations
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_package_install_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of package installation operations
- **Tags**: `sub_name`, `pub_name`, `editable`

### Invalidation Metrics

#### `sling_distribution_journal_subscriber_invalidation_process_request_count` (Counter)
- **Type**: Counter
- **Description**: Number of invalidation process requests
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_invalidation_process_success_count` (Counter)
- **Type**: Counter
- **Description**: Number of successful invalidation process operations
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_invalidation_process_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of invalidation process operations
- **Tags**: `sub_name`, `pub_name`, `editable`

### Distribution Duration Metrics

#### `sling_distribution_journal_subscriber_request_distributed_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of distributing a distribution package. The timer starts when the package is enqueued and stops when the package is successfully imported
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_package_journal_distribution_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration that a package spent in the distribution journal. The timer starts when the package is enqueued and stops when the package is consumed
- **Tags**: `sub_name`, `pub_name`, `editable`

### Status Reporting Metrics

#### `sling_distribution_journal_subscriber_send_stored_status_duration` (Timer)
- **Type**: Timer
- **Unit**: Milliseconds
- **Description**: Duration of sending a stored package status
- **Tags**: `sub_name`, `pub_name`, `editable`

### Readiness Metrics

#### `sling_distribution_journal_subscriber_readiness_duration` (Timer)
- **Type**: Timer
- **Unit**: Seconds
- **Description**: Duration until the subscriber becomes ready, categorized by the reason for readiness
- **Tags**: `sub_name`, `pub_name`, `editable`, `ready_reason`
- **Ready Reason Values**:
  - `IDLE`: Subscriber became ready after being idle for a configured duration
  - `MAX_RETRIES`: Subscriber became ready because retries exceeded maximum
  - `LATENCY`: Subscriber became ready because package message latency was below acceptable limit
  - `FORCE`: Subscriber became ready due to forced readiness after timeout

### FileVault (FV) Metrics

#### `sling_distribution_journal_subscriber_fv_message_count` (Counter)
- **Type**: Counter
- **Description**: Count of FileVault progress messages during package import/extraction operations. FileVault (Apache Jackrabbit FileVault) is used for installing content packages.
- **Tags**: `sub_name`, `pub_name`, `editable`

#### `sling_distribution_journal_subscriber_fv_error_count` (Counter)
- **Type**: Counter
- **Description**: Count of FileVault errors encountered during package import/extraction operations
- **Tags**: `sub_name`, `pub_name`, `editable`

---

## Metric Types Reference

- **Counter**: A cumulative metric that represents a single monotonically increasing counter
- **Gauge**: A metric that represents a single numerical value that can arbitrarily go up and down
- **Histogram**: A metric that samples observations and counts them in configurable buckets
- **Meter**: A metric that measures the rate of events over time
- **Timer**: A metric that measures both the rate that a particular piece of code is called and the distribution of its duration

