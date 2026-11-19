---
title: "Flink Connector: Lance REST Catalog"
slug: "/flink-connector/flink-catalog-lance"
keyword: "flink connector lance rest catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Apache Gravitino Flink connector supports accessing Lance REST catalogs without requiring users to manually specify storage paths for every table in Flink SQL.
Tables and schemas are managed through Apache Gravitino, which serves as the authoritative metadata source and automatically populates table locations and Lance connector properties for query execution.

## Capabilities

### DDL and DML Operations

- `CREATE CATALOG`
- `CREATE DATABASE` / `DROP DATABASE`
- `CREATE TABLE` / `DROP TABLE` / `ALTER TABLE`
- `SELECT` queries across tables without manually configuring `'connector' = 'lance'` or `'path'` on every query.

### Automatic Table Path Resolution

When querying or creating tables in a Gravitino Lance catalog:
1. The table's storage location is automatically retrieved from Gravitino metadata (or derived from the catalog's warehouse location: `<warehouse>/<database>/<table>`).
2. Flink table scan options are automatically populated with `'connector' = 'lance'` and `'path' = '<location>'`.
3. Storage configurations prefixed with `lance.storage.` configured on the catalog are automatically propagated to table options.

## Getting Started

### Prerequisites

* Flink 1.19, 1.20, or 2.1
* Apache Gravitino server with Lance REST service enabled (or an external Lance REST server)
* Gravitino Flink connector runtime JAR matching your Flink version in Flink's `lib` directory:

| Flink version | Scala | Gravitino connector runtime artifact                                   |
|---------------|-------|------------------------------------------------------------------------|
| 1.19          | 2.12  | `gravitino-flink-connector-runtime-1.19_2.12-${gravitino-version}.jar` |
| 1.20          | 2.12  | `gravitino-flink-connector-runtime-1.20_2.12-${gravitino-version}.jar` |
| 2.1           | n/a   | `gravitino-flink-connector-runtime-2.1-${gravitino-version}.jar`       |

Replace `${gravitino-version}` with your Gravitino release version.

## Configuration Options

| Option key | Type | Default | Description | Required |
|---|---|---|---|---|
| `type` | string | (none) | Must be set to `gravitino-lance`. | Yes |
| `uri` | string | (none) | The Lance REST server endpoint URI (e.g., `http://localhost:8090/lance`). | Yes |
| `catalog-backend` | string | `rest` | The Lance catalog backend type (`rest` or `dir`). | No |
| `impl` | string | `rest` | The Lance catalog implementation (`rest` or `dir`). | No |
| `warehouse` | string | (none) | Base warehouse storage location (e.g., `s3://bucket/warehouse` or `hdfs://namenode:8020/lance`). | No |
| `default-database` | string | `default` | The default database name. | No |
| `lance.storage.*` | string | (none) | Storage configurations passed to the underlying Lance storage subsystem (e.g., `lance.storage.s3.endpoint`). | No |

## SQL Example

### 1. Create and Use Catalog

```sql
-- Create a Lance REST catalog in Flink SQL
CREATE CATALOG lance_catalog WITH (
    'type' = 'gravitino-lance',
    'uri' = 'http://localhost:8090/lance',
    'warehouse' = 's3://my-bucket/warehouse'
);

USE CATALOG lance_catalog;
```

### 2. Create Database and Table

```sql
CREATE DATABASE IF NOT EXISTS my_database;
USE my_database;

-- Notice: No manual storage path configuration is required!
CREATE TABLE users (
    id BIGINT,
    name STRING,
    created_at TIMESTAMP(3)
);
```

### 3. Query Table

```sql
-- The Flink connector automatically resolves 'connector' = 'lance' and 'path' = 's3://my-bucket/warehouse/my_database/users'
SELECT * FROM users WHERE id > 100;
```
