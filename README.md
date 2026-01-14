[![Apache Sling](https://sling.apache.org/res/logos/sling.png)](https://sling.apache.org)

&#32;[![Build Status](https://ci-builds.apache.org/job/Sling/job/modules/job/sling-org-apache-sling-distribution-journal/job/master/badge/icon)](https://ci-builds.apache.org/job/Sling/job/modules/job/sling-org-apache-sling-distribution-journal/job/master/)&#32;[![Test Status](https://img.shields.io/jenkins/tests.svg?jobUrl=https://ci-builds.apache.org/job/Sling/job/modules/job/sling-org-apache-sling-distribution-journal/job/master/)](https://ci-builds.apache.org/job/Sling/job/modules/job/sling-org-apache-sling-distribution-journal/job/master/test/?width=800&height=600)&#32;[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=apache_sling-org-apache-sling-distribution-journal&metric=coverage)](https://sonarcloud.io/dashboard?id=apache_sling-org-apache-sling-distribution-journal)&#32;[![Sonarcloud Status](https://sonarcloud.io/api/project_badges/measure?project=apache_sling-org-apache-sling-distribution-journal&metric=alert_status)](https://sonarcloud.io/dashboard?id=apache_sling-org-apache-sling-distribution-journal)&#32;[![JavaDoc](https://www.javadoc.io/badge/org.apache.sling/org.apache.sling.distribution.journal.svg)](https://www.javadoc.io/doc/org.apache.sling/org.apache.sling.distribution.journal)&#32;[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.sling/org.apache.sling.distribution.journal/badge.svg)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.sling%22%20a%3A%22org.apache.sling.distribution.journal%22)&#32;[![distribution](https://sling.apache.org/badges/group-distribution.svg)](https://github.com/apache/sling-aggregator/blob/master/docs/groups/distribution.md) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

# Apache Sling Journal based Content Distribution - Core

This module is part of the [Apache Sling](https://sling.apache.org) project, it implements [Apache Sling Content Distribution](https://sling.apache.org/documentation/bundles/content-distribution.html) agents on a message journal.
Please refer to the [documentation](docs/documentation.md) to learn about the use case and general design.

## Metrics

This bundle exposes comprehensive metrics for monitoring publisher and subscriber operations. All metrics are exposed through the Apache Sling Metrics Service. Metric names use underscores instead of dots and are prefixed with `sling_`.

### Publisher Metrics

Publisher metrics (prefixed with `sling_distribution_journal_publisher_`) track package export, request handling, package building, queue operations, and subscriber discovery:
- **Package Export**: `sling_distribution_journal_publisher_exported_package_size` (histogram)
- **Request Handling**: `sling_distribution_journal_publisher_accepted_requests`, `sling_distribution_journal_publisher_dropped_requests` (meters)
- **Package Building**: `sling_distribution_journal_publisher_build_package_duration`, `sling_distribution_journal_publisher_enqueue_package_duration` (timers)
- **Queue Operations**: `sling_distribution_journal_publisher_queue_size` (gauge), `sling_distribution_journal_publisher_queue_cache_fetch_count`, `sling_distribution_journal_publisher_queue_access_error_count` (counters)
- **Subscriber Discovery**: `sling_distribution_journal_publisher_subscriber_count` (gauge)

### Subscriber Metrics

Subscriber metrics (prefixed with `sling_distribution_journal_subscriber_`) track package import, processing, errors, and distribution duration:
- **Package Import**: `sling_distribution_journal_subscriber_imported_package_size`, `sling_distribution_journal_subscriber_imported_package_duration`, `sling_distribution_journal_subscriber_current_import_duration` (histogram/timer/gauge)
- **Package Status**: `sling_distribution_journal_subscriber_package_status_count` (counter) with status tags (IMPORTED, REMOVED, REMOVED_FAILED)
- **Error Tracking**: `sling_distribution_journal_subscriber_transient_import_errors`, `sling_distribution_journal_subscriber_permanent_import_errors`, `sling_distribution_journal_subscriber_import_errors`, `sling_distribution_journal_subscriber_current_retries` (counters/gauge)
- **Processing**: Pre/post-processing and installation metrics (counters/timers)
- **Distribution Duration**: `sling_distribution_journal_subscriber_request_distributed_duration`, `sling_distribution_journal_subscriber_package_journal_distribution_duration` (timers)
- **Readiness**: `sling_distribution_journal_subscriber_readiness_duration` (timer) with ready reason tags
- **FileVault**: `sling_distribution_journal_subscriber_fv_message_count`, `sling_distribution_journal_subscriber_fv_error_count` (counters)

All metrics are tagged with `sub_name`/`pub_name` and `editable` (for subscriber metrics) to enable filtering and aggregation.

For a complete list of all metrics with detailed descriptions, see the [Metrics Overview](docs/METRICS_OVERVIEW.md).
