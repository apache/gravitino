# Apache Gravitino Roadmap

## 2024 Roadmap

As of March 2024, this is the current roadmap for the Apache Gravitino project. Please note that Gravitino is an open-source project and relies on the collective efforts of both community contributors and paid developers to shape its future features and direction. While we strive to keep this roadmap up-to-date, it is best seen as a general guide for future developments rather than an exhaustive list of planned features. The Gravitino community may decide to alter the project's direction or prioritize other features that are not listed here.

## First half of 2024 (January-June) - Make Gravitino ready for production environment

### Q1 (January-March)

- Release Gravitino 0.4.0 with support for partitions, improved Gravitino UI, Kerberos and query optimisation.
- Release Gravitino 0.5.0 with non-tabular data catalog, messaging data catalog support, and Spark engine support. (Now mid April)
- Implement user/privilege support (basic framework).
- Encourage and support community’s work on integrating MySQL, Python API, Apache Doris, Apache Kafka and Apache Paimon.

### Q2 (April-June)

- Join the Apache Software Foundation as an incubating project.
- Release Gravitino 0.6.0 with native storage support (HMS compatible), and privilege support (Apache Ranger).
- Implement support for a basic data compliance framework.
- Continue to encourage and support community work.

## Second half of 2024 (July-December) - Improve Apache Gravitino features

### Q3 (July-September)

- Implement centralized mechanisms for managing and enforcing access control policies across Gravitino deployments.
- Deploy a standalone REST catalog server compatible with Iceberg.
- Develop and release a connector for Flink, enhancing Gravitino’s compatibility with stream processing frameworks.
- Enable metadata tagging for easier resource categorization and management.
- Release Gravitino 0.6.1

### Q4 (October-December)

- Provide support for Filesystem in Userspace (FUSE) and Container Storage Interface (CSI).
- Introduce cloud storage support, including secure credential vending to simplify access management in cloud-native setups.
- Develop and release a comprehensive auditing framework for tracking and logging data access.
- Release Gravitino 0.7.0 with focus on advanced security and cloud integration features.

## 2025 Roadmap

- Streamline dataset versioning, metadata organization, and secure sharing across teams.
- Add storage and indexing for vector data, improving compatibility with ML workflows.
- Improve support for large text datasets tailored for LLM applications.

Apache, Apache Doris, Apache Kafka, Apache Spark, Apache Ranger, Apache Paimon are either registered trademarks or trademarks of The Apache Software Foundation in the United States and other countries.
