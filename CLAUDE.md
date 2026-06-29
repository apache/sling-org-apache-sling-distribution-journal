# Apache Sling Distribution Journal

## Overview

This bundle implements journal-based Sling Content Distribution: a Publisher agent running on AEM Author serializes content packages and writes them to an append-only message journal (e.g., Apache Kafka); Subscriber agents running on AEM Publish instances consume those messages and import the packages into their local JCR repositories. The design uses event sourcing so that many subscribers can consume independently at their own pace, subscriber queues are derived from a single journal partition rather than stored separately, and consumer offsets are persisted atomically with the JCR import so content is imported exactly once. See `docs/documentation.md` for the full design document.

## Tech Stack

- Java 11, OSGi bundle (bnd-maven-plugin, `sling-bundle-parent` v66)
- Depends on `sling-org-apache-sling-distribution-api`, `distribution-core`, and `distribution-journal-messages`
- Apache Jackrabbit Vault for content serialization; Apache Oak JCR for offset storage
- Sling Commons Metrics (Dropwizard Metrics) for operational metrics
- Testing: JUnit 4, Mockito 4, Awaitility, Sling Mock (Oak-backed), Logback

## Build & Test Commands

```
mvn clean install                         # build and unit tests
mvn test -Dtest=BookKeeperTest            # single test class
mvn test -Dtest=PubQueueCacheTest         # queue cache tests
```

There are no integration tests in this bundle; all tests are unit/mock-based.

## Architecture

Main packages under `org.apache.sling.distribution.journal`:

| Package | Purpose |
|---|---|
| `impl.publisher` | Publisher agent: serializes content, writes `PackageMessage` to journal, exposes queues |
| `impl.subscriber` | Subscriber agent: reads `PackageMessage` from journal, delegates import to `BookKeeper` |
| `impl.discovery` | Subscriber heartbeat loop: periodically sends `DiscoveryMessage` so the publisher knows subscriber offsets |
| `impl.precondition` | `Precondition` SPI: guards import (e.g., "wait until package is visible in blob store") |
| `impl.event` | Fires OSGi events after distribution; `DistributionFailureEvent` |
| `bookkeeper` | `BookKeeper` — atomically imports a content package and saves the processed journal offset in the same JCR session. Also manages retry logic and sends `PackageStatusMessage` |
| `queue` / `queue.impl` | `PubQueueProvider`, `OffsetQueue`, `PubQueueCache` — derive per-subscriber queue views from the in-memory journal cache without storing duplicates |
| `shared` | Cross-cutting utilities: `Topics` constants, `ExponentialBackOff`, `AgentId`, `OnlyOnLeader`, JMX registration |
| `metrics` | Tagged metrics helpers wrapping Sling Commons Metrics |

**Journal topics** (configured via `Topics` constants, prefixed at deployment time, typically `aemdistribution_`):
- `package` — one `PackageMessage` per distribution action
- `status` — import outcomes from subscribers
- `discovery` — subscriber heartbeats (~every 10s)
- `command` — clear/admin commands
- `event` — `PackageDistributedMessage` after successful distribution

**Key data flow**:
1. Author calls `DistributionAgent.execute()` → Publisher agent builds content package → writes `PackageMessage` to `package` topic.
2. Subscriber's consumer loop reads `PackageMessage` from `package` topic → `BookKeeper.importPackage()` imports into JCR and saves offset under `/var/sling/distribution/journal/stores`.
3. Each subscriber periodically writes a `DiscoveryMessage` to `discovery` topic with its last processed offset.
4. Publisher reads `DiscoveryMessage`s to derive per-subscriber queue views via `PubQueueCache`.

Large content packages (deep trees > ~1 MB) are stored as binaries in the shared Oak blob store under `/var/sling/distribution/journal/packages`; the `PackageMessage` carries only a reference (`pkgBinaryRef`).

## Conventions & Gotchas

- **Offset storage is atomic with import**: `BookKeeper` saves the journal offset and the imported content in a single `Session.save()`. Never split these into two saves or you risk duplicate imports after a restart.
- The publisher queue is an **in-memory cache** (`PubQueueCache`) that is fully recycled every 12 hours. Queue views are rebuilt from the journal on next access. Do not assume queue entries survive a bundle restart.
- **Error queues do not support retry**: unlike the Sling Jobs-based distribution, the journal error queue is read-only from the publisher's perspective. To retry a failed package, issue a new distribution request.
- The `MessagingProvider` (broker adapter, e.g., Kafka) is an **external dependency** — this bundle only uses the SPI from `distribution-journal-messages`. The concrete adapter must be deployed separately and register a `MessagingProvider` OSGi service; without it the `JournalAvailable` condition is not satisfied and agents won't start.
- `OnlyOnLeader` uses Sling Discovery to ensure certain tasks (e.g., large-package cleanup) run on only one cluster node. If Sling Discovery is not configured, this guard may prevent those tasks from running.
- The `bnd.bnd` at the repo root sets the service user mapping for the `bookkeeper` and `importer` sub-services. These must exist in the AEM instance as mapped to `replication-service` (or a custom mapping) for imports to succeed.
- The `docs/` directory contains architecture diagrams (PNG + draw.io XML). They are excluded from the Apache RAT license check via `pom.xml` configuration.
