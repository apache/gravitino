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
* Before creating a catalogue, ensure you have already created a Metalake. If you still need to, please follow the Metalake creation steps.

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
* `uri`: This configuration is required, and it can be either a Hive URI or a JDBC URI.
* `warehouse`: This configuration is required, and can point to a file system such as HDFS.
* Other configuration parameters can be added to the "properties" section and passed down to the underlying system.
* If you are using the JDBC catalog implementation, include "jdbc-user" and "jdbc-password" as required configurations.
* If you intend to use the JDBC connector, add the corresponding JDBC driver to the `catalogs/lakehouse-iceberg/libs` directory in the classpath.

### catalog configuration

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

Example JSON:

```json
{
    "name": "test_table",
    "comment": "my test table",
    "columns": [
        {
            "name": "id",
            "type": "int",
            "comment": "id column comment"
        },
        {
            "name": "name",
            "type": "string",
            "comment": "name column comment"
        },
        {
            "name": "age",
            "type": "int",
            "comment": "age column comment"
        },
        {
            "name": "dt",
            "type": "date",
            "comment": "dt column comment"
        }
    ],
    "partitions": [
        {
            "strategy": "identity",
            "fieldName": ["dt"]
        }
    ],
    "sortOrders": [
        {
            "expression": {
                "expressionType": "field",
                "fieldName": ["age"]
            },
            "direction": "asc",
            "nullOrdering": "first"
        }
    ],
    "properties": {
        "k1" : "v1"
    }
}
```

* `name`: The name of the Iceberg table to be created.
* `comment`: Optional, user custom Iceberg table comment.
* `columns`: The columns of the Iceberg table.
* `partitions`: Optional, the partitions of the Iceberg table, above example is a partitioned table with `dt` column.
* `sortOrders`: Optional, equivalent to the `SORTED BY` clause in Iceberg DDL, above example table data is sorted in increasing order of `age` in each bucket.
* `properties`: The properties of the Iceberg table. For more property information see the following properties table. Other properties will be passed down to the underlying Iceberg table parameters.

Iceberg lacks distribution support. For buckets, consider using partitions instead.

### table properties
Gravitino reserves the following fields and they cannot be passed in properties.

| Configuration item                                                            | Description                                                                                                                                                                                |
|-------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `comment`                                                                     | The table comment.                                                                                                                                                                         |
| `creator`                                                                     | The table creator.                                                                                                                                                                         |
| `location`                                                                    | Iceberg location for table storage.                                                                         |
| `current-snapshot-id`                                                         | The snapshot representing the current state of the table.                    |
| `cherry-pick-snapshot-id`                                                     | Selecting a specific snapshots in a merge operation. |
| `sort-order`                                                                  | Selecting a specific snapshots in a merge operation.         |
| `identifier-fields`                                                                            | The identifier field(s) for defining the table.                                                                                                                                           |

Regarding Iceberg's properties, you can refer to [official documentation](https://iceberg.apache.org/docs/1.3.1/configuration/).

You can use Iceberg as a catalog for managing your data in Gravitino. If you encounter any issues or need further assistance, refer to the Gravitino documentation or seek help from the support team.
