---
title: Manage table partitions
slug: /manage-table-partitions
date: 2024-02-03
keyword: table partition management
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Although many catalogs inherently manage partitions automatically,
there are scenarios where manual partition management is necessary.
Usage scenarios like managing the TTL (Time-To-Live) of partition data,
gathering statistics on partition metadata, and optimizing queries through partition pruning.
For these reasons, Apache Gravitino provides capabilities of partition management.

## Requirements and limitations

- Partition management is based on the partitioned table, so please ensure that you are operating on a partitioned table.

The following table shows the partition operations supported across various catalogs in Gravitino:

<table>
<thead>
<tr>
  <th>Operation</th>
  <th>Hive catalog</th>
  <th>Iceberg catalog</th>
  <th>JDBC-MySQL catalog</th>
  <th>JDBC-PostgreSQL catalog</th>
  <th>JDBC-Doris catalog</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Add Partition</td>
  <td>&#10004;</td>
  <td>&#10008;</td>  
  <td>&#10008;</td>  
  <td>&#10008;</td>  
  <td>&#10004;</td>  
</tr>
<tr>
  <td>Get Partition by Name</td>
  <td>&#10004;</td>
  <td>&#10008;</td>
  <td>&#10008;</td>
  <td>&#10008;</td>
  <td>&#10004;</td>
</tr>
<tr>
  <td>List Partition Names</td>
  <td>&#10004;</td>
  <td>&#10008;</td>
  <td>&#10008;</td>
  <td>&#10008;</td>
  <td>&#10004;</td>
</tr>
<tr>
  <td>List Partition</td>
  <td>&#10004;</td>
  <td>&#10008;</td>
  <td>&#10008;</td>
  <td>&#10008;</td>
  <td>&#10004;</td>
</tr>
<tr>
  <td>List Partition</td>
  <td>&#10004;</td>
  <td>&#10008;</td>
  <td>&#10008;</td>
  <td>&#10008;</td>
  <td>&#10004;</td>
</tr>
</tbody>
</table>

:::tip[WELCOME FEEDBACK]
If you need additional partition management support for a specific catalog,
please feel free to [create an issue](https://github.com/apache/gravitino/issues/new/choose)
in the [Gravitino repository](https://github.com/apache/gravitino).
:::

## Partition operations

### Add partition

You must match the partition types you want to add with the table's
[partitioning types](../../table-partitioning-bucketing-sort-order-indexes.md#table-partitioning).
Gravitino currently supports adding the following partition types:

- *identity*: An identity partition represents a result of identity partitioning.
- *range*: A range partition represents a result of range partitioning.
- *list*: A list partition represents a result of list partitioning.

The following is an example:

<Tabs groupId="partitions">
<TabItem value="identity" label="identity">

```json
{
  "type": "identity",
  "name": "dt=2008-08-08/country=us",
  "fieldNames": [
    [
      "dt"
    ],
    [
      "country"
    ]
  ],
  "values": [
    {
      "type": "literal",
      "dataType": "date",
      "value": "2008-08-08"
    },
    {
      "type": "literal",
      "dataType": "string",
      "value": "us"
    }
  ]
}
```

:::note
The values of the field `values` must be the same ordering as the values of `fieldNames`.

When adding an identity partition to a partitioned Hive table,
the partition name specified is ignored.
This is because Hive generates the partition name based on field names and values.
:::

</TabItem>
<TabItem value="range" label="range">

```json
{
  "type": "range",
  "name": "p20200321",
  "upper": {
    "type": "literal",
    "dataType": "date",
    "value": "2020-03-21"
  },
  "lower": {
    "type": "literal",
    "dataType": "null",
    "value": "null"
  }
}
```

</TabItem>
<TabItem value="list" label="list">

```json
{
  "type": "list",
  "name": "p202204_California",
  "lists": [
    [
      {
        "type": "literal",
        "dataType": "date",
        "value": "2022-04-01"
      },
      {
        "type": "literal",
        "dataType": "string",
        "value": "Los Angeles"
      }
    ],
    [
      {
        "type": "literal",
        "dataType": "date",
        "value": "2022-04-01"
      },
      {
        "type": "literal",
        "dataType": "string",
        "value": "San Francisco"
      }
    ]
  ]
}
```

:::note
Each list in the lists must have the same length.
The values in each list must correspond to the field definitions in the listi partitioning.
:::

</TabItem>
</Tabs>

For Java examples:

<Tabs groupId="partitions">
<TabItem value="identity" label="Identity">

```java
Partition partition = Partitions.identity(
    "dt=2008-08-08/country=us",
    new String[][] {{"dt"}, {"country"}},
    new Literal[] {
        Literals.dateLiteral(LocalDate.parse("2008-08-08")), Literals.stringLiteral("us")
    },
    Maps.newHashMap());
```

:::note
The values are in the same order as the field names.

When adding an identity partition to a partitioned Hive table,
the partition name specified is ignored.
This is because Hive generates the partition name based on field names and values.
:::

</TabItem>
<TabItem value="range" label="Range">

```java
Partition partition = Partitions.range(
    "p20200321",
    Literals.dateLiteral(LocalDate.parse("2020-03-21")),
    Literals.NULL,
    Maps.newHashMap());
```

</TabItem>

<TabItem value="list" label="List">

```java
Partition partition = Partitions.list(
    "p202204_California",
    new Literal[][] {
        {
            Literals.dateLiteral(LocalDate.parse("2022-04-01")),
            Literals.stringLiteral("Los Angeles")
        },
        {
            Literals.dateLiteral(LocalDate.parse("2022-04-01")),
            Literals.stringLiteral("San Francisco")
        }
    },
    Maps.newHashMap());
```

:::note
Each list in the lists must have the same length.
The values in each list must correspond to the field definitions in the list partitioning.
:::

</TabItem>
</Tabs>

You can add a partition to a partitioned table by sending a `POST` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{partitioned_table_name}/partitions` endpoint
or by using the Gravitino client SDKs.

The following is an example of adding an identity partition to a Hive partitioned table:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >partition.json
{
  "partitions": [
    {
      "type": "identity",
      "fieldNames": [
        [
          "dt"
        ],
        [
          "country"
        ]
      ],
      "values": [
        {
          "type": "literal",
          "dataType": "date",
          "value": "2008-08-08"
        },
        {
          "type": "literal",
          "dataType": "string",
          "value": "us"
        }
      ]
    }
  ]
}
EOF 

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@partition.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables/mytable/partitions
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("metalake")
    .build();

// This assumes that you have a partitioned table named "metalake.catalog.schema.table".
Partition addedPartition = gravitinoClient
    .loadCatalog("catalog")
    .asTableCatalog()
    .loadTable(NameIdentifier.of("schema", "table"))
    .supportPartitions()
    .addPartition(
         Partitions.identity(
             new String[][] {{"dt"}, {"country"}},
             new Literal[] {
             Literals.dateLiteral(LocalDate.parse("2008-08-08")), Literals.stringLiteral("us")},
             Maps.newHashMap()));
```

</TabItem>
</Tabs>

### Get a partition by name

You can get a partition by its name via sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions/{partition}` endpoint
or by using the Gravitino client SDKs.
The following is an example of getting a partition by its name:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables/mytable/partitions/p20200321
```

:::tip
If the partition name contains special characters, you should use
[URL encoding](https://en.wikipedia.org/wiki/Percent-encoding#Reserved_characters).
For example, if the partition name is `dt=2008-08-08/country=us`,
you should use `dt%3D2008-08-08%2Fcountry%3Dus` in the URL.
:::

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
  .builder("http://127.0.0.1:8090")
  .withMetalake("mymetalake")
  .build();

// This assumes that you have a partitioned table named "mymetalake.mycatalog.myschema.mytable".
Partition Partition = 
  gravitinoClient
      .loadCatalog("mycatalog")
      .asTableCatalog()
      .loadTable(NameIdentifier.of("myschema", "mytable"))
      .supportPartitions()
      .getPartition("partition_name");
```

</TabItem>
</Tabs>

### List partition names under a partitioned table

You can list all partition names under a partitioned table by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions` endpoint
or by using the Gravitino client SDKs.

The following is an example of listing all the partition names under a partitioned table:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables/mytable/partitions
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("mymetalake")
    .build();

// This assumes that you have a partitioned table named "mymetalake.mycatalog.myschema.mytable".
String[] partitionNames = gravitinoClient
    .loadCatalog("mycatalog")
    .asTableCatalog()
    .loadTable(NameIdentifier.of("myschema", "mytable"))
    .supportPartitions()
    .listPartitionNames();
```

</TabItem>
</Tabs>

### List partitions under a partitioned table

If you want to get more detailed information about the partitions under a partitioned table,
you can list all partitions under a partitioned table by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions` endpoint
or by using the Gravitino client SDKs.

The following is an example of listing all the partitions under a partitioned table:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables/mytable/partitions?details=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a partitioned table named "mymetalake.mycatalog.myschema.mytable".
Partition[] partitions = gravitinoClient
    .loadCatalog("mycatalog")
    .asTableCatalog()
    .loadTable(NameIdentifier.of("myschema", "mytable"))
    .supportPartitions()
    .listPartitions();
```

</TabItem>
</Tabs>

### Drop a partition

You can drop a partition by its name via sending a `DELETE` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions/{partition}` endpoint
or by using the Gravitino client SDKs.

The following is an example of dropping a partition by its name:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables/mytable/partitions/p20200321
```

:::tip
If the partition name contains special characters, you should use
[URL encoding](https://en.wikipedia.org/wiki/Percent-encoding#Reserved_characters).
For example, if the partition name is `dt=2008-08-08/country=us`,
you should use `dt%3D2008-08-08%2Fcountry%3Dus` in the URL.
:::

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("mymetalake")
    .build();

// This assumes that you have a partitioned table named "mymetalake.mycatalog.myschema.mytable".
Partition Partition = gravitinoClient
    .loadCatalog("mycatalog")
    .asTableCatalog()
    .loadTable(NameIdentifier.of("myschema", "mytable"))
    .supportPartitions()
    .dropPartition("partition_name");
```
</TabItem>
</Tabs>
