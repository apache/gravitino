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
object storage like S3, GCS, and Azure Blob Storage, you can put the hadoop object store jar like
hadoop-aws into the `$GRAVITINO_HOME/catalogs/hadoop/libs` directory to enable the support.
Gravitino itself hasn't yet tested the object storage support, so if you have any issue,
please create an [issue](https://github.com/datastrato/gravitino/issues).

Note that Gravitino uses Hadoop 3 dependencies to build Hadoop catalog. Theoretically, it should be
compatible with both Hadoop 2.x and 3.x, since Gravitino doesn't leverage any new features in
Hadoop 3. If there's any compatibility issue, please create an [issue](https://github.com/datastrato/gravitino/issues).

## Catalog

### Catalog properties

| Property Name | Description                                     | Default Value | Required | Since Version |
|---------------|-------------------------------------------------|---------------|----------|---------------|
| `location`    | The storage location managed by Hadoop catalog. | (none)        | No       | 0.5.0         |

### Catalog operations

Refer to [Catalog operations](./manage-fileset-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

The Hadoop catalog supports creating, updating, deleting, and listing schema.

### Schema properties

| Property name | Description                                    | Default value | Required | Since Version |
|---------------|------------------------------------------------|---------------|----------|---------------|
| `location`    | The storage location managed by Hadoop schema. | (none)        | No       | 0.5.0         |

### Schema operations

Refer to [Schema operation](./manage-fileset-metadata-using-gravitino.md#schema-operations) for more details.

## Fileset

### Fileset capabilities

- The Hadoop catalog supports creating, updating, deleting, and listing filesets.

### Fileset properties

None.

### Fileset operations

Refer to [Fileset operations](./manage-fileset-metadata-using-gravitino.md#fileset-operations) for more details.
