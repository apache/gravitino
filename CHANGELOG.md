<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Changelog

All notable changes to Apache Gravitino are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For the complete list of commits in each release, see the
[GitHub Releases](https://github.com/apache/gravitino/releases) page.

---

## [Unreleased] — 2.0.0-SNAPSHOT

_Development is ongoing on the `main` branch._

---

## [1.3.0] — 2026-06-28

Full Changelog: [v1.2.0...v1.3.0](https://github.com/apache/gravitino/compare/v1.2.0...v1.3.0)

### Highlights

- **AWS Glue Catalog and Engine Connector Support**: Added AWS Glue Catalog support and Trino/Spark connector adapters, allowing Hive and Iceberg metadata in Glue to be governed and queried through Gravitino.
- **Unified View Management**: Added a unified view definition model and view support across Hive, Iceberg, and Apache Paimon, covering APIs, persistence, Java client support, and Web UI management.
- **Core Cache and Authorization Consistency**: Improved multi-node cache correctness with version-validated authorization caches, entity-change-log tracking, and global cache invalidation.
- **Enterprise-Grade Iceberg REST Catalog**: Added federated Iceberg REST Catalog support, nested multi-level namespaces, vended-credential refresh, freshness-aware table loading, and asynchronous cleanup.

### Added

- Logical view management as first-class, versioned entities across supported catalogs.
- Hierarchical (multi-level nested) namespaces in core REST server and Iceberg REST Catalog.
- AWS Glue catalog with schema/table CRUD, native Iceberg table support, and Trino/Spark adapters.
- Built-in identity provider, password hashing, and Basic authentication for local deployments.
- Function authorization, group-aware ownership, scoped `MANAGE_GRANTS` delegation.
- Asynchronous hard-deletion cleanup and vended-credential refresh for Iceberg REST Catalog.
- Python client authorization management and relational catalog support.
- Trino: `CREATE TABLE AS SELECT`, UDF adaptation, session-credential forwarding.
- Flink: View support for Iceberg and Paimon catalogs; support for Flink 1.19 and 1.20.
- Health-check endpoints for Gravitino and IRC; JSON formatter for audit logs.
- New Hologres JDBC catalog (Alibaba Cloud Hologres).

### Changed

- Docker image install path moved from `/root/gravitino` to `/opt/gravitino`.
- Iceberg REST JDBC catalog defaults to strict mode (404 for non-existent namespaces).
- Iceberg table metadata cache enabled by default with increased capacity.
- Sensitive catalog properties hidden from catalog load responses by default.
- The `web-v2` UI is now the default UI.
- Hadoop upgraded from 2.10.2 to 3.3.6; legacy `hadoop2` dependency line removed.

### Fixed

- Glue catalog edge cases, Trino catalog rollback/drop behavior.
- Iceberg REST hierarchical namespace drops, staged create failures.
- Multi-admin IdP initialization, stale role bindings, authorization cache issues.
- Audit timestamp precision and internal cross-server audit attribution.
- Web UI view listing, table/view navigation 404s, copy SQL behavior.
- Docker image build inputs, published Docker image install paths.

---

## [1.2.1] — 2026-05-07

Full Changelog: [v1.2.0...v1.2.1](https://github.com/apache/gravitino/compare/v1.2.0...v1.2.1)

### Fixed

- Stability improvements and bug fixes backported from `main`.

---

## [1.2.0] — 2026-04-07

Full Changelog: [v1.1.0...v1.2.0](https://github.com/apache/gravitino/compare/v1.1.0...v1.2.0)

### Added

- Continued metadata governance and AI asset management improvements.
- Enhanced fileset management and catalog interoperability.

---

## [1.1.1] — 2026-04-28

Full Changelog: [v1.1.0...v1.1.1](https://github.com/apache/gravitino/compare/v1.1.0...v1.1.1)

### Fixed

- Stability improvements and bug fixes backported from `main`.

---

## [1.1.0] — 2026-02-03

Full Changelog: [v1.0.1...v1.1.0](https://github.com/apache/gravitino/compare/v1.0.1...v1.1.0)

### Added

- Client and catalog improvements for smoother integration workflows.
- Deployment image updates to Eclipse Temurin.

---

## [1.0.1] — 2025-12-02

Full Changelog: [v1.0.0...v1.0.1](https://github.com/apache/gravitino/compare/v1.0.0...v1.0.1)

### Fixed

- Stability improvements post-1.0.0 release.
- Bug fixes and reliability improvements across connectors and metadata services.

---

## [1.0.0] — 2025-09-15

Full Changelog: [v0.9.1...v1.0.0](https://github.com/apache/gravitino/compare/v0.9.1...v1.0.0)

### Highlights

- **First stable major release** of Apache Gravitino.
- Metadata-driven action system for automated governance and workflow triggers.
- Unified access control across catalogs, engines, and metadata operations.
- Enhanced model metadata capabilities for AI/ML workloads.
- Improved connector packaging, integration workflows, and runtime stability.

---

## [0.9.1] — 2025-08-11

Full Changelog: [v0.9.0-incubating...v0.9.1](https://github.com/apache/gravitino/compare/v0.9.0-incubating...v0.9.1)

### Fixed

- Incremental improvements and stability fixes post-incubation graduation.

---

## [0.9.0-incubating] — 2025-06-02

Full Changelog: [v0.8.0-incubating...v0.9.0-incubating](https://github.com/apache/gravitino/compare/v0.8.0-incubating...v0.9.0-incubating)

### Added

- AI and metadata governance improvements, including model and large-dataset handling.
- Fileset management and catalog interoperability enhancements.

---

## [0.8.0-incubating] — 2025-03-10

Full Changelog: [v0.7.0-incubating...v0.8.0-incubating](https://github.com/apache/gravitino/compare/v0.7.0-incubating...v0.8.0-incubating)

### Added

- **Model catalog**: Expanding support for AI/ML metadata use cases.
- Fileset FUSE support and secure credential vending for cloud and on-premise deployments.
- New connectors: Flink–Iceberg, Flink–Paimon, Spark–Paimon.

---

## [0.7.0-incubating] — 2024-12-16

Full Changelog: [v0.6.1-incubating...v0.7.0-incubating](https://github.com/apache/gravitino/compare/v0.6.1-incubating...v0.7.0-incubating)

### Added

- Filesystem in Userspace (FUSE) and Container Storage Interface (CSI) support.
- Cloud storage support with secure credential vending.
- Comprehensive auditing framework for tracking and logging data access.

---

## [0.6.1-incubating] — 2024-10-14

Full Changelog: [v0.6.0-incubating...v0.6.1-incubating](https://github.com/apache/gravitino/compare/v0.6.0-incubating...v0.6.1-incubating)

### Fixed

- Bug fixes and stability improvements for the 0.6.x release line.

---

## [0.6.0-incubating] — 2024-08-05

Full Changelog: [v0.5.1...v0.6.0-incubating](https://github.com/apache/gravitino/compare/v0.5.1...v0.6.0-incubating)

### Added

- **Joined the Apache Software Foundation** as an incubating project.
- Native storage (HMS-compatible) support.
- Apache Ranger privilege integration.
- Basic data-compliance framework.

---

## [0.5.1] — 2024-06-10

Full Changelog: [v0.5.0...v0.5.1](https://github.com/apache/gravitino/compare/v0.5.0...v0.5.1)

### Fixed

- Patch release with bug fixes and stability improvements.

---

## [0.5.0] — 2024-04-15

Full Changelog: [v0.4.0...v0.5.0](https://github.com/apache/gravitino/compare/v0.4.0...v0.5.0)

### Added

- Non-tabular data catalog support.
- Messaging data catalog support.
- Spark engine integration.

---

## [0.4.0] — 2024-02-12

Full Changelog: [v0.3.1...v0.4.0](https://github.com/apache/gravitino/compare/v0.3.1...v0.4.0)

### Added

- Partition support.
- UI improvements.
- Kerberos authentication.
- Query optimisation improvements.

---

## [0.3.1] — 2024-01-08

Full Changelog: [v0.3.0...v0.3.1](https://github.com/apache/gravitino/compare/v0.3.0...v0.3.1)

### Fixed

- Patch release with stability improvements.

---

## [0.3.0] — 2023-12-11

Full Changelog: [v0.2.0...v0.3.0](https://github.com/apache/gravitino/compare/v0.2.0...v0.3.0)

### Added

- Early catalog connectors and core metadata model improvements.

---

## [0.2.0] — 2023-10-23

### Added

- Initial public release of Gravitino.
- Core metadata management framework.
- REST API server.
- Hive catalog support.

---

<!-- Link definitions -->
[Unreleased]: https://github.com/apache/gravitino/compare/v1.3.0...HEAD
[1.3.0]: https://github.com/apache/gravitino/releases/tag/v1.3.0
[1.2.1]: https://github.com/apache/gravitino/releases/tag/v1.2.1
[1.2.0]: https://github.com/apache/gravitino/releases/tag/v1.2.0
[1.1.1]: https://github.com/apache/gravitino/releases/tag/v1.1.1
[1.1.0]: https://github.com/apache/gravitino/releases/tag/v1.1.0
[1.0.1]: https://github.com/apache/gravitino/releases/tag/v1.0.1
[1.0.0]: https://github.com/apache/gravitino/releases/tag/v1.0.0
[0.9.1]: https://github.com/apache/gravitino/releases/tag/v0.9.1
[0.9.0-incubating]: https://github.com/apache/gravitino/releases/tag/v0.9.0-incubating
[0.8.0-incubating]: https://github.com/apache/gravitino/releases/tag/v0.8.0-incubating
[0.7.0-incubating]: https://github.com/apache/gravitino/releases/tag/v0.7.0-incubating
[0.6.1-incubating]: https://github.com/apache/gravitino/releases/tag/v0.6.1-incubating
[0.6.0-incubating]: https://github.com/apache/gravitino/releases/tag/v0.6.0-incubating
[0.5.1]: https://github.com/apache/gravitino/releases/tag/v0.5.1
[0.5.0]: https://github.com/apache/gravitino/releases/tag/v0.5.0
[0.4.0]: https://github.com/apache/gravitino/releases/tag/v0.4.0
[0.3.1]: https://github.com/apache/gravitino/releases/tag/v0.3.1
[0.3.0]: https://github.com/apache/gravitino/releases/tag/v0.3.0
[0.2.0]: https://github.com/apache/gravitino/releases/tag/v0.2.0
