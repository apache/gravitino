---
title: "Apache Gravitino Glossary"
date: 2023-11-28
license: "This software is licensed under the Apache License version 2."
---

## API

- Application Programming Interface, defining the methods and protocols for interacting with a server.

## AWS

- Amazon Web Services, a cloud computing platform provided by Amazon.

## AWS Glue

- A compatible implementation of the Hive Metastore Service (HMS).

## GPG/GnuPG

- Gnu Privacy Guard or GnuPG is an open-source implementation of the OpenPGP standard.
  It is usually used for encrypting and signing files and emails.

## HDFS

- **HDFS** (Hadoop Distributed File System) is an open-source distributed file system.
  It is a key component of the Apache Hadoop ecosystem.
  HDFS is designed as a distributed storage solution to store and process large-scale datasets.
  It features high reliability, fault tolerance, and excellent performance.

## HTTP port

- The port number on which a server listens for incoming connections.

## IP address

- Internet Protocol address, a numerical label assigned to each device in a computer network.

## JDBC

- Java Database Connectivity, an API for connecting Java applications to relational databases.

## JDBC URI

- The JDBC connection address specified in the catalog configuration.
  It usually includes components such as the database type, host, port, and database name.

## JDK

- The software development kit for the Java programming language.
  A JDK provides tools for compiling, debugging, and running Java applications.

## JMX 

- Java Management Extensions provides tools for managing and monitoring Java applications.

## JSON

- JavaScript Object Notation, a lightweight data interchange format.

## JSON Web Token

- See [JWT](#jwt).

## JVM

- A virtual machine that enables a computer to run Java applications.
  A JVM implements an abstract machine that is different from the underlying hardware.

## JVM instrumentation 

- The process of adding monitoring and management capabilities to the [JVM](#jvm).
  The purpose of instrumentation is mostly for the collection of performance metrics.

## JVM metrics 

- Metrics related to the performance and behavior of the [Java Virtual Machine](#jvm).
  Some useful metrics are memory usage, garbage collection, and buffer pool metrics.

## JWT

- A compact, URL-safe representation for claims between two parties.

## KEYS file

- A file containing public keys used to sign previous releases, necessary for verifying signatures.

## PGP signature

- A digital signature generated using the Pretty Good Privacy (PGP) algorithm.
  The signature is typically used for validating the authenticity of a file.

## REST

- A set of architectural principles for designing networked applications.

## REST API

- Representational State Transfer (REST) Application Programming Interface.
  A set of rules and conventions for building and interacting with Web services using standard HTTP methods.

## SHA256 checksum

- A cryptographic hash function used to verify the integrity of files.

## SHA256 checksum file

- A file containing the SHA256 hash value of another file, used for verification purposes.

## SQL

- A programming language used to manage and manipulate relational databases.

## SSH

- Secure Shell, a cryptographic network protocol used for secure communication over a computer network.

## URI

- Uniform Resource Identifier, a string that identifies the name or resource on the internet.

## YAML

- YAML Ain't Markup Language, a human-readable file format often used for structured data.

## Amazon Elastic Block Store (EBS)

- A scalable block storage service provided by Amazon Web Services (AWS).

## Apache Gravitino

- An open-source software platform originally created by Datastrato.
  It is designed for high-performance, geo-distributed, and federated metadata lakes.
  Gravitino can manage metadata directly in different sources, types, and regions,
  providing unified metadata access for data and AI assets.

## Apache Gravitino configuration file (gravitino.conf)

- The configuration file for the Gravitino server, located in the `conf` directory.
  It follows the standard properties file format and contains settings for the Gravitino server.

## Apache Hadoop

- An open-source distributed storage and processing framework.

## Apache Hive

- An open-source data warehousing software project.
  It provides SQL-like query language for managing and querying large datasets.

## Apache Iceberg

- An open-source, versioned table format for large-scale data processing.

## Apache Iceberg Hive catalog

- The **Iceberg Hive catalog** is a metadata service designed for the Apache Iceberg table format.
  It allows external systems to interact with Iceberg metadata using a Hive metastore thrift client.

## Apache Iceberg JDBC catalog

- The **Iceberg JDBC catalog** is a metadata service designed for the Apache Iceberg table format.
  It enables external systems to interact with Iceberg metadata service using [JDBC](#jdbc).

## Apache Iceberg REST catalog

- The **Iceberg REST Catalog** is a metadata service designed for the Apache Iceberg table format.
  It enables external systems to interact with Iceberg metadata service using a [REST API](#rest-api).

## Apache License version 2

- A permissive, open-source software license written by The Apache Software Foundation.

## Authentication mechanism

- The method used to verify the identity of users and clients accessing a server.

## Binary distribution package

- A software package containing the compiled executables for distribution and deployment.

## Catalog

- A collection of metadata from a specific metadata source.

## Catalog provider 

- The specific system or technology used to store and manage metadata catalogs.

## Columns

- The individual fields or attributes of a table.
  Each column has some properties like name, data type, comment, and nullability.

## Continuous integration (CI)

- The practice of automatically building and testing code changes when they are committed to version control.

## Contributor covenant

- A widely-used and recognized code of conduct for open-source communities.
  It provides guidelines for creating a welcoming and inclusive environment for all contributors.

## Dependencies

- External libraries or modules required by a project for its compilation and features.

## Distribution

- A packaged and deployable version of the software.

## Docker

- A platform for developing, shipping, and running applications in containers.

## Docker container

- A lightweight, standalone package that includes everything needed to run a software.
  A container compiles an application with its dependencies and runtime for distribution.

## Docker Hub

- A cloud-based registry service for Docker containers.
  Users can publish, browse and download containerized software using this service.

## Docker image

- A lightweight, standalone package that includes everything needed to run a software.
  A Docker image typically comprises the code, runtime, libraries, and system tools.

## Dockerfile

- A configuration file for building a Docker image.
  A Dockerfile contains instructions to build a standard image for distributing a software.

## Dropwizard metrics

- A Java library for measuring the performance of applications and providing support for various metric types.

## Environment variables

- Variables used to customize the runtime configuration for a process.

## Geo-distributed

- The distribution of data or services across multiple geographic locations.

## Git

- A distributed version control system used for tracking software artifacts.

## GitHub

- A web-based platform for version control and community collaboration using Git.

## GitHub Actions

- A continuous integration and continuous deployment (CI/CD) service provided by GitHub.
  GitHub Actions are used for automating the build, test, and deployment workflows.

## GitHub labels

- Labels assigned to GitHub issues or pull requests for organization or workflow automation.

## GitHub pull request

- A proposed change to a GitHub repository submitted by a user.

## GitHub repository

- The location where GitHub stores a project's source code and related files.

## GitHub workflow

- A series of automated steps that are triggered by certain events on a GitHub repository.

## Gradle

- A automation tool for building, testing, and deploying projects.

## Gradlew

- A Gradle wrapper script, used for executing Gradle commands.

## Hashes

- Cryptographic hash values generated from some data.
  A typical use case is to verify the integrity of a file.

## Headless

- A system without a local console.

## Identity fields

- Fields in tables that define the identity of the records.
  In the scope of a table, the identity fields are used as the unique identifier of a row.

## Integration tests

- Tests for the correctness and compatibility of a software.
  It is typically conducted when integrating a component into a larger system.

## Java Database Connectivity (JDBC)

- See [JDBC](#jdbc)

## Java Development Kits (JDKs)

- See [JDK](#jdk)

## Java Management Extensions

- See [JMX](#jmx)

## Java Toolchain

- A Gradle feature for detecting and managing JDK versions. 

## Java Virtual Machine

- See [JVM](#jvm)

## Key pair

- A pair of cryptographic keys, including a public key used for verification and a private key used for signing.

## Lakehouse

- **Lakehouse** is a modern data management architecture that combines elements of data lakes and data warehouses.
  It aims to provide a unified platform for storing, managing, and analyzing both raw unstructured data
  (similar to data lakes) and curated structured data.

## Manifest

- A list of files and their associated metadata that collectively define the structure and content of a release or distribution.

## Merge operation

- A process in Iceberg that involves combining changes from multiple snapshots into a new snapshot.

## Metalake

- The top-level container for metadata. 
  Typically, a metalake is a tenant-like mapping to an organization or a company.
  All the catalogs, users, and roles are associated with one metalake. 

## Metastore

- A central repository that stores metadata for a data warehouse.

## Module

- A distinct and separable part of a project.

## Open authorization / OAuth

- A standard protocol for authorization that allows third-party applications to authenticate a user.
  The application doesn't need to access the user credentials.

## OrbStack

- A tool mentioned as an alternative to Docker for macOS when running Gravitino integration tests.

## Private key

- A confidential key used for signing, decryption, or other operations that should remain confidential.

## Properties

- Configurable settings and attributes associated with catalogs, schemas, and tables.
  The property settings influence the behavior and storage of the corresponding entities.

## Protocol buffers (protobuf)

- A method developed by Google for serializing structured data, similar to XML or JSON.
  It is often used for efficient and extensible communication between systems.

## Public key

- An openly shared key used for verification, encryption, or other operations intended for public knowledge.

## Representational State Transfer

- See [REST](#rest)

## RocksDB

- An open source key-value storage database.

## Schema

- A logical container for organizing tables in a database.

## Secure Shell

- See [SSH](#ssh)

## Security group

- A virtual firewall for your instance to control inbound and outbound traffic.

## Serde

- A serialization/deserialization library.
  It can be used for transforming data between a tabular format and a format suitable for storage or transmission.

## Snapshot

- A point-in-time capture of the state of an Iceberg table, representing a specific version of the table.

## Sort order

- The arrangement of data within a Hive table, specified by expression or direction.

## Spotless

- A tool or process used to enforce code formatting standards and apply automatic formatting to code.

## Structured Query Language

- See [SQL](#sql)

## Table

- A structured set of data elements stored in columns and rows.

## Thrift

- A network protocol used for communication with Hive Metastore Service (HMS).

## Token

- A **token** in the context of computing and security is a small, indivisible unit of data. 
  Tokens play a crucial role in various domains, including authentication and authorization.

## Trino

- A query engine for big data processing.

## Trino connector

- A connector module for integrating Gravitino with Trino.

## Ubuntu

- A Linux distribution based on Debian, widely used for cloud computing and servers.

## Unit test

- A type of software testing where individual components or functions of a program are tested.
  Unit tests help to ensure that the component or function works as expected in isolation.

## Verification

- The process of confirming the authenticity and integrity of a release.
  This is usually done by checking its signature and associated hash values.

## Web UI

- A graphical interface accessible through a web browser.


