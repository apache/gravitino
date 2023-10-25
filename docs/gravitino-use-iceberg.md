---
title: "How to use Gravitino to manage Iceberg metadata"
date: 2023-10-18
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---

## Using Iceberg as a Catalog in Gravitino

Gravitino provides the ability to use Iceberg as a catalog for managing your data. This guide will walk you through the steps to create a catalog using Iceberg within Gravitino.

### Capabilities

* Worked as a catalog proxy, supports HiveCatalog and JdbcCatalog for now
* The integrated Iceberg version used is 1.3.1.
* Before you create a catalog, make sure you have already created a Metalake. If you haven't done so, please follow the Metalake creation steps.

## Creating an Iceberg Catalog

To create an Iceberg catalog, use the following steps:

Submit a catalog JSON example to the Gravitino server using the URL format:

```shell
http://{GravitinoServerHost}:8090/api/metalakes/{Your_metalake_name}/catalogs
```

   Example JSON:

   ```json
   {
       "name": "test",
       "comment": "my test catalog",
       "type": "RELATIONAL",
       "provider": "lakehouse-iceberg",
       "properties": {
           "catalog-backend": "jdbc",
           "uri": "jdbc:mysql://127.0.0.1:3306/metastore_db?createDatabaseIfNotExist=true",
           "jdbc-user": "iceberg",
           "jdbc-password": "iceberg",
           "warehouse": "file:///tmp/iceberg"
       }
   }
   ```

* `provider`: Set this to "lakehouse-iceberg" to use Iceberg as the catalog provider.
* `catalog-backend`: This configuration represents the catalog mode used by Iceberg. You can choose from "hive" and "jdbc".
* `uri`: This is a required configuration, and it can be either a Hive URI or a JDBC URI.
* `warehouse`: This is a required configuration and can point to a file system such as HDFS.
* Other configuration parameters can be added to the "properties" section and passed down to the underlying system.
* If you are using the JDBC catalog implementation, make sure to include "jdbc-user" and "jdbc-password" as required configurations.
* If you intend to use the JDBC connector, you need to add the corresponding JDBC driver to the `catalogs/lakehouse-iceberg/libs` directory in the classpath.

### configuration

| Configuration item                | Description                                      | value                                                                                                |
|-----------------------------------|--------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `catalog-backend` | Catalog backend of Gravitino Iceberg             | `hive` or `jdbc`                                                                                     |
| `uri` | Hive metadata address or JDBC connection address | `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/` or `jdbc:mysql://127.0.0.1:3306/test`        |
| `warehouse` | Warehouse directory of Catalog                   | `/user/hive/warehouse-hive/`  or `hdfs://namespace/hdfs/path`                                        |

### HDFS configuration

You can place (`core-site.xml` and `hdfs-site.xml`) in the `catalogs/lakehouse-iceberg/conf` directory, and it will be automatically loaded as the default HDFS configuration. Of course, you can also pass the respective key-value pairs in the JSON parameters under 'properties' for personalized configuration.

## After the catalog is initialized

You can manage and operate on tables using the following URL format:

   ```shell
   http://{GravitinoServerHost}:8090/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables
   ```

Now you can use Iceberg as a catalog for managing your data in Gravitino. If you encounter any issues or need further assistance, refer to the Gravitino documentation or seek help from the support team.
