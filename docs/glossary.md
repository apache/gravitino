---
title: "Apache Gravitino Glossary"
date: 2023-11-28
license: "This software is licensed under the Apache License version 2."
---

## Apache Hadoop

- An open-source distributed storage and processing framework.

## Apache Hive

- An open-source data warehousing and SQL-like query language software project for managing and querying large datasets.

## Apache Iceberg

- An open-source, versioned table format for large-scale data processing.

## Apache License version 2

- A permissive, open-source software license written by The Apache Software Foundation.

## API

- Application Programming Interface, defining the methods and protocols for interacting with a server.

## Authentication mechanism

- The method used to verify the identity of users and clients accessing a server.

## AWS

- Amazon Web Services, a cloud computing platform provided by Amazon.

## AWS Glue

- A compatible implementation of the Hive Metastore Service (HMS).

## Binary distribution package

- A package containing the compiled and executable version of the software, ready for distribution and deployment.

## Catalog

- A collection of metadata from a specific metadata source.

## Catalog provider 

- The specific system or technology used to store and manage metadata catalogs.

## Columns

- The individual fields or attributes of a table, specifying details such as name, data type, comment, and nullability.

## Continuous integration (CI)

- The practice of automatically building, testing, and validating code changes when they are committed to version control.

## Contributor covenant

- A widely-used and recognized code of conduct for open-source communities. It provides guidelines for creating a welcoming and inclusive environment for all contributors.

## Dependencies

- External libraries or modules required by a project for its compilation and features.

## Distribution

- A packaged and deployable version of the software.

## Docker

- A platform for developing, shipping, and running applications in containers.

## Docker container

- A lightweight, standalone, executable package that includes everything needed to run a piece of software, including the code, runtime, libraries, and system tools.

## Docker Hub

- A cloud-based registry service for Docker containers, allowing users to share and distribute containerized applications.

## Docker image

- A lightweight, standalone, and executable package that includes everything needed to run a piece of software, including the code, runtime, libraries, and system tools.

## Docker file

- A configuration file used to create a Docker image, specifying the base image, dependencies, and commands for building the image.

## Dropwizard Metrics

- A Java library for measuring the performance of applications and providing support for various metric types.

## Amazon Elastic Block Store (EBS)

- A scalable block storage service provided by Amazon Web Services.

## Environment variables

- Variables used to pass information to running processes.

## Geo-distributed

- The distribution of data or services across multiple geographic locations.

## GitHub

- A web-based platform for version control and collaboration using Git.

## GitHub Actions

- A continuous integration and continuous deployment (CI/CD) service provided by GitHub, used for automating build, test, and deployment workflows.

## GitHub labels

- Tags assigned to GitHub issues or pull requests for organization, categorization, or workflow automation.

## GitHub pull request

- A proposed change to a repository submitted by a user through the GitHub platform.

## GitHub repository

- The location where GitHub stores a project's source code and related files.

## GitHub workflow

- A series of automated steps defined in a YAML file that runs in response to events on a GitHub repository.

## Git

- A version control system used for tracking changes and collaborating on source code.

## GPG/GnuPG

- Gnu Privacy Guard or GnuPG, an open-source implementation of the OpenPGP standard, used for encrypting and signing files and emails.

## Gradle

- A build automation tool for building, testing, and deploying projects.

## Gradlew

- A Gradle wrapper script, used for executing Gradle commands without installing Gradle separately.

## Apache Gravitino

- An open-source software platform originally created by Datastrato for high-performance, geo-distributed, and federated metadata lakes. Designed to manage metadata directly in different sources, types, and regions, providing unified metadata access for data and AI assets.

## Apache Gravitino configuration file (gravitino.conf)

- The configuration file for the Gravitino server, located in the `conf` directory. It follows the standard property file format and contains settings for the Gravitino server.

## Hashes

- Cryptographic hash values generated from the contents of a file, often used for integrity verification.

## HDFS

- **HDFS** (Hadoop Distributed File System) is an open-source, distributed file system and a key component of the Apache Hadoop ecosystem. It is designed to store and process large-scale datasets, providing high reliability, fault tolerance, and performance for distributed storage solutions.

## Headless

- A system without a graphical user interface.

## HTTP port

- The port number on which a server listens for incoming connections.

## Apache Iceberg Hive catalog

- The **Iceberg Hive catalog** is a specialized metadata service designed for the Apache Iceberg table format, allowing external systems to interact with Iceberg metadata via a Hive metastore thrift client.

## Apache Iceberg REST catalog

- The **Iceberg REST Catalog** is a specialized metadata service designed for the Apache Iceberg table format, allowing external systems to interact with Iceberg metadata via a RESTful API.

## Apache Iceberg JDBC catalog

- The **Iceberg JDBC Catalog** is a specialized metadata service designed for the Apache Iceberg table format, allowing external systems to interact with Iceberg metadata using JDBC (Java Database Connectivity).

## Identity fields

- Fields in tables that define the identity of the table, specifying how rows in the table are uniquely identified.

## Integration tests

- Tests designed to ensure the correctness and compatibility of software when integrated into a unified system.

## IP address

- Internet Protocol address, a numerical label assigned to each device participating in a computer network.

## Java Database Connectivity (JDBC)

- Java Database Connectivity, an API for connecting Java applications to relational databases.

## Java Development Kits (JDKs)

- Software development kits for the Java programming language, including tools for compiling, debugging, and running Java applications.

## Java Toolchain

- A feature introduced in Gradle to detect and manage JDK versions. 

## JDBC URI

- The JDBC connection address specified in the catalog configuration, including details such as the database type, host, port, and database name.

## JMX 

- Java Management Extensions provides tools for managing and monitoring Java applications.

## JSON

- JavaScript Object Notation, a lightweight data interchange format.

## JWT(JSON Web Token)

- A compact, URL-safe means of representing claims between two parties.

##  Java Virtual Machine (JVM)

- A virtual machine that enables a computer to run Java applications, providing an abstraction layer between the application and the underlying hardware.

## JVM metrics 

- Metrics related to the performance and behavior of the Java Virtual Machine (JVM), including memory usage, garbage collection, and buffer pool metrics.

## JVM instrumentation 

- The process of adding monitoring and management capabilities to the Java Virtual Machine, allowing for the collection of performance metrics.

## Key pair

- A pair of cryptographic keys, including a public key used for verification and a private key used for signing.

## KEYS file

- A file containing public keys used to sign previous releases, necessary for verifying signatures.

## Lakehouse

- **Lakehouse** refers to a modern data management architecture that combines elements of data lakes and data warehouses. It aims to provide a unified platform for storing, managing, and analyzing both raw unstructured data (similar to data lakes) and curated structured data.

## Manifest

- A list of files and associated metadata that collectively define the structure and content of a release or distribution.

## Merge operation

- A process in Iceberg that involves combining changes from multiple snapshots into a new snapshot.

## Metalake

- The top-level container for metadata. Typically, a metalake is a tenant-like mapping to an organization or a company. All the catalogs, users, and roles are under one metalake. 

## Metastore

- A central repository that stores metadata for a data warehouse.

## Module

- A distinct and separable part of a project.

## OrbStack

- A tool mentioned as an alternative to Docker for macOS when running Gravitino integration tests.

## Open authorization / OAuth

- A standard protocol for authorization that allows third-party applications to access user data without exposing user credentials.

## PGP Signature

- A digital signature generated using the Pretty Good Privacy (PGP) algorithm, confirming the authenticity of a file.

## Private key

- A confidential key used for signing, decryption, or other operations that should remain confidential.

## Properties

- Configurable settings and attributes associated with catalogs, schemas, and tables, to influence their behavior and storage.

## Protocol buffers (protobuf)

- A method developed by Google for serializing structured data, similar to XML or JSON. It is often used for efficient and extensible communication between systems.

## Public key

- An openly shared key used for verification, encryption, or other operations intended for public knowledge.

## Representational State Transfer (REST)

- A set of architectural principles for designing networked applications.

## REST API (Representational State Transfer Application Programming Interface)

- A set of rules and conventions for building and interacting with web services using standard HTTP methods.

## RocksDB

- An open source key-value pair storage database.

## Schema

- A logical container for organizing tables in a database.

## Secure Shell (SSH)

- Secure Shell, a cryptographic network protocol used for secure communication over a computer network.

## Security group

- A virtual firewall for your instance to control inbound and outbound traffic.

## Serde

- A Serialization/Deserialization library responsible for transforming data between a tabular format and a format suitable for storage or transmission.

## SHA256 checksum

- A cryptographic hash function used to verify the integrity of files.

## SHA256 checksum file

- A file containing the SHA256 hash value of another file, used for verification purposes.

## Snapshot

- A point-in-time capture of the state of an Iceberg table, representing a specific version of the table.

## Sort order

- The arrangement of data within a Hive table, specified by expression or direction.

## Spotless

- A tool or process used to enforce code formatting standards and apply automatic formatting to code.

## Structured Query Language (SQL)

- A programming language used to manage and manipulate relational databases.

## Table

- A structured set of data elements stored in columns and rows.

## Token

- A **token** in the context of computing and security commonly refers to a small, indivisible unit of data. Tokens play a crucial role in various domains, including authentication, authorization, and cryptographic systems.

## Thrift protocol

- The network protocol used for communication with Hive Metastore Service (HMS).

## Trino

- A query engine for big data processing.

## Trino connector

- A connector module for integrating Gravitino with Trino.

## Trino Apache Gravitino connector documentation

-  Documentation providing information on using the Trino connector to access metadata in Gravitino.

## Ubuntu

- A Linux distribution based on Debian, widely used for cloud computing and servers.

## Unit test

- A type of testing where individual components or functions of a program are tested to ensure they work as expected in isolation.

## URI

- Uniform Resource Identifier, a string that identifies the name or resource on the internet.

## Verification

- The process of confirming the authenticity and integrity of a release by checking its signature and associated hashes.

## WEB UI

- A graphical interface accessible through a web browser.

## YAML

- YAML Ain't Markup Language, a human-readable data serialization format often used for configuration files.
