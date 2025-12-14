# Apache Gravitino Roadmap

As of November 2025, this is the current roadmap for the Apache Gravitino project. Please note that Gravitino is an open-source project and relies on the collective efforts of both community contributors and paid developers to shape its future features and direction. While we strive to keep this roadmap up-to-date, it is best seen as a general guide for future developments rather than an exhaustive list of planned features. The Gravitino community may decide to alter the project's direction or prioritize other features that are not listed here.

We actively welcome community participation in defining and implementing the items on this roadmap. Contributors are encouraged to propose new features, discuss design ideas, refine priorities, and bring forward use cases or integrations that matter to them. The direction of Gravitino is intentionally open and community-driven, and we invite all contributors to help shape its future.

# Apache Gravitino Roadmap

## 2026 Roadmap

To be updated as development direction is defined

---

## 2025 Roadmap

### Q4 (October–December 2025)

- Release Gravitino 1.0.1 with stability improvements.
- Update deployment images to Eclipse Temurin for improved security and compatibility.
- Deliver client and catalog improvements for smoother integration workflows.
- Address bug fixes and reliability issues across connectors and metadata services.

### Q3 (July–September 2025)

- Release Gravitino 1.0.0, marking the first stable major version.
- Release Gravitino 0.9.1 with incremental improvements.
- Introduce the metadata-driven action system for automated governance and workflow triggers.
- Deliver unified access control across catalogs, engines, and metadata operations.
- Enhance model metadata capabilities for AI/ML workloads.
- Improve connector packaging, integration workflows, and runtime stability.

### Q2 (April–June 2025)

- **Graduate from the Apache Incubator and officially become an Apache Top Level Project (June 3, 2025).**
- Release Gravitino 0.9.0.
- Improve AI and metadata governance, including model and large-dataset handling.
- Deliver enhancements to fileset management and catalog interoperability.

### Q1 (January–March 2025)

- Release Gravitino 0.8.0.
- Introduce the model catalog, expanding support for AI/ML metadata use cases.
- Add fileset FUSE support and secure credential vending for cloud and on-premise deployments.
- Deliver new connectors:  
  - Flink–Iceberg  
  - Flink–Paimon  
  - Spark–Paimon

---

## 2024 Roadmap

### Q4 (October–December 2024)

- Provide support for Filesystem in Userspace (FUSE) and Container Storage Interface (CSI).
- Introduce cloud storage support, including secure credential vending for cloud-native setups.
- Develop and release a comprehensive auditing framework for tracking and logging data access.
- Release Gravitino 0.7.0 with a focus on advanced security and cloud integration features.

### Q3 (July–September 2024)

- Implement centralized mechanisms for managing and enforcing access control policies across Gravitino deployments.
- Deploy a standalone REST catalog server compatible with Iceberg.
- Develop and release a connector for Flink, enhancing compatibility with stream-processing frameworks.
- Enable metadata tagging for resource categorization and management.
- Release Gravitino 0.6.1.

### Q2 (April–June 2024)

- Join the Apache Software Foundation as an incubating project.
- Release Gravitino 0.6.0 with native storage (HMS-compatible) support and Apache Ranger privilege integration.
- Implement a basic data-compliance framework.
- Continue to encourage and support community integration work.

### Q1 (January–March 2024)

- Release Gravitino 0.4.0 with support for partitions, UI improvements, Kerberos, and query optimisation.
- Release Gravitino 0.5.0 with non-tabular data catalog support, messaging data catalog support, and Spark engine integration (mid-April).
- Implement the initial user/privilege framework.
- Encourage and support community integration efforts:  
  MySQL, Python API, Apache Doris, Apache Kafka, Apache Paimon.