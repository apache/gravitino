---
title: "How to use Gravitino to manage Hive metadata"
date: 2023-10-20
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---
# Using Hive as a Catalog in Gravitino

Gravitino offers the functionality to utilize Hive as a catalog for metadata management. This guide will lead you through the process of creating a catalog using Hive in Gravitino.

### Requirements:
* The Hive catalog requires a Hive metastore service(HMS), or a compatible implementation of the Hive metastore, such as AWS Glue.
* The Gravitino must have network access to the Hive metastore service with the Thrift protocol.
* Apache Hive 2.x is supported.
* Before you create a Hive catalog, make sure you have already created a Metalake. If you haven't done so, please follow the Metalake creation steps.

## Creating an Hive Catalog
To create a Hive catalog, use the following steps:

Submit a catalog JSON example to the Gravitino server using the URL format:

```
http://{GravitinoServerHost}:{GravitinoServerPort}/api/metalakes/{Your_metalake_name}/catalogs
```
Example JSON:

```json
   {
       "name": "test",
       "comment": "my test Hive catalog",
       "type": "RELATIONAL",
       "provider": "hive",
       "properties": {
           "metastore.uris": "thrift://127.0.0.1:9083"
       }
   }
```

- `provider`: Set this to "hive" to use Hive as the catalog provider.
- `metastore.uris`: This is a required configuration, and it can be the Hive metastore service URIs.
- Other configuration parameters can be added to the "properties" section, and they will be passed down to the underlying Hive metastore.

#### configuration

| Configuration item                | Description                                                                              | value                                                         |
|-----------------------------------|------------------------------------------------------------------------------------------|---------------------------------------------------------------|
| `metastore.uris` | Hive metastore service address, separate multiple addresses with commas                  | `thrift://127.0.0.1:9083`                                     |
| `client.pool-size` | The maximum number of Hive metore clients in the pool for Gravitino. 1 by default value  | 1                                                      |

## After the catalog is initialized
You can manage and operate on tables using the following URL format:

```
http://{GravitinoServerHost}:{GravitinoServerPort}/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables
```
Now you can use Hive as a catalog for managing your metadata in Gravitino. If you encounter any issues or need further assistance, refer to the Gravitino documentation or seek help from the support team.