---
title: "Hadoop catalog"
slug: /hadoop-catalog
date: 2024-4-2
keyword: hadoop catalog
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

Hadoop catalog is a fileset catalog that using Hadoop Compatible File System (HCFS) to manage
the storage location of the fileset. Currently, it supports local filesystem and HDFS. For
object stores like S3, ADLS, and GCS, we haven't yet tested.

Note that the Hadoop catalog is built against Hadoop 3, it should be compatible with both Hadoop
2.x and 3.x, since we don't leverage any new features in Hadoop 3. If there's any compatibility
issue, please let us know.

## Catalog

### Catalog properties

| Property Name | Description                                     | Default Value | Required | Since Version |
|---------------|-------------------------------------------------|---------------|----------|---------------|
| `location`    | The storage location managed by Hadoop catalog. | (none)        | No       | 0.5.0         |

### Catalog operations

Refer to [Catalog operations](./manage-fileset-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

The Hadoop catalog supports creating, updating, and deleting schema.

### Schema properties

| Property name | Description                                   | Default value | Required | Since Version |
|---------------|-----------------------------------------------|---------------|----------|---------------|
| `location`    | The storage location managed by Hadoop schema | (none)        | No       | 0.5.0         |

### Schema operations

Refer to [Schema operation](./manage-fileset-metadata-using-gravitino.md#schema-operations) for more details.

## Fileset

### Fileset capabilities

- The Hadoop catalog supports creating, updating, and deleting filesets.

### Fileset properties

No.
