# RFC-2: Metadata Connector Design

## Design Prerequisites

The Graviton receives multiple data sources as input, stores associated metadata, 
and outputs this information through a RESTful API to support the Graviton UI.

When first designing the Graviton, one of the major decisions we had to make 
was whether we would store the metadata we collected or extract it on request. 
Our service needs to support `high throughput` and `low latency` reads, and if we delegate 
this responsibility to metadata sources, we need all data sources to support `high throughput` 
and `low latency` reads, which introduces complexity and risk.
For example, a Vertica query that gets a table schema often takes a few seconds to process, 
making it unsuitable for visualization. Similarly, our Hive metastore manages 
all of Hive's metadata, making it risky to require high throughput read requests. 

Because the Graviton supports so many different data sources of metadata, 
we decided to store the metadata in the Graviton architecture itself. 

In addition, while most use cases require new metadata, they do not need to see 
the metadata change in real time, making periodic collected or extract possible.

We also separate the request service layer from the data source connector layer so that 
each layer runs in a separate process, as shown in below figure:
![rfc-2-01.png](rfc-2-01.png)

This isolates the two layers, which reduces collateral impact. 
For example, a metadata data connection job may use a large amount of system resources, 
which may affect the SLA of the api at the request service layer. 

In addition, the data connection layer is less sensitive to outages than the 
Graviton's request service layer, ensuring that outdated metadata will still 
be served if the data connection layer goes down, thus minimizing the impact on users.

## Connectors

Connectors can connect different data source, such as relational databases or file systems or WebServices,
and extract metadata from them. We will map data source to `Graviton` metadata schema, table and column concept. 

### ConnectorFactory

Instances of the connector are created by the `ConnectorFactory`, The Factory creates its own connector for each 
different dada source, The connector factory is responsible for creating an instance of a connector object based 
on the `create` function  
`Connector create(String lakehouse, String tenant, String zone, Map<String, String> config, ConnectorContext context)`.

### Typical Connectors
`Graviton` prioritize implement very high usage and important connector.

#### Hive Connector
Follow up to add.

#### JDBC Connector
Follow up to add.

#### File Connector
Follow up to add.


## Event-listen vs. Scheduled

Our next challenge was to determine the most effective and efficient way to collect or extract metadata
from several different and completely disparate data sources.

Creating metadata change event lister is faster and low cost than period scheduled,
However, Not all data sources support event notification,
So we chose to use the period scheduled mode first and optimize later.

We started by creating crawlers that regularly collect or extract schema from various data sources
and microservices that generate metadata information about the dataset.

We needed to collect metadata information frequently in a scalable way without blocking
other crawler tasks. To do this, we deployed the crawlers to different machines and
needed to coordinate efficiently among the crawlers in a distributed manner.
We considered configuring [Quartz](https://github.com/quartz-scheduler/quartz)
in cluster mode for distributed scheduling.

In addition, to accommodate future cloud deployment models,
we need to deploy the crawler to different hosts and multiple cloud in kubernetes pod containers.

## Environment isolation

Because each connector plug-in will contain many dependent JAR packages that easy cause conflicts,
We need to have the plug-in run in an isolation environment.
So we need store each plug-in and its associated dependency packages in a separate directory.
All jar packages in this directory are dynamical loaded through `ClassLoader` function using java reflate mechanism.

In additionally, packages dependency isolation's functionality also to better supported for running in cloud environment.
Each plug-in can be stand-alone deployment independence in the kubernetes pod container,
Allow each data source connector scala different as needed.

## Metadata

### QualifiedName

The `QualifiedName` class is fully qualified name that references a source of data.

| Field Name  | Field Type          | Description                        | Optional |
|-------------|---------------------|------------------------------------| -------- |
| lakehouse   | uint64              | Lakehouse name of the data source. | Required |
| tenant      | string              | Tenant name of the data source.    | Required |
| zone        | string              | Zone name of the data source.      | Required |
| table       | string              | Table name of the data source.     | Optional |
| mview       | string              | View name of the data sourc.       | Optional |
| partition   | string              | The partition name of the table.   | Optional |
| column      | string              | Column name of the data source.    | Optional |
| column_type | Type Category       | Type category of the Column.       | Optional |

### Metadata Types

The Type interface in `Graviton` is used to implement a type store them into repository.
`Graviton` comes with a number of built-in types, like `VarcharType` and `BigintType`.
The ParametricType interface is used to provide type parameters for types,
to allow types like `VARCHAR(10)` or `DECIMAL(10, 4)`.

### Type Signature

The TypeSignature class is used to uniquely identify a type.
It contains the type name and the type parameters (if it’s parametric), and its literal parameters.
Every type map to Substrait's type system.

Substrait’s type system can be referred here https://substrait.io/types/type_system/.
In this way, different engines can read and write the same data based on the `Graviton` schema.
Instead of each engine having to build a separate external table,
multiple engines can be seamlessly connected in the future.

### Type Entity

#### Type Category

Type category is enum type, it's used to indicate the type category of the type. contains the following values:
`VOID`, `BOOLEAN`, `BYTE`, `SHORT`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `STRING`,
`DATE`, `TIMESTAMP`, `TIMESTAMPLOCALTZ`, `BINARY`, `DECIMAL`, `VARCHAR`, `CHAR`,
`INTERVAL_YEAR_MONTH`, `INTERVAL_DAY_TIME`, `UNKNOWN`.

| Field Name | Field Type | Description                | Optional |
|------------|------------|----------------------------| -------- |
| name       | string     | Type category Name.        | Required |
| enum_value | uint32     | Type category enum's value | Required |

#### Parametric Type

Parametric Type is used to provide type parameters for types, to allow types like `VARCHAR(10)` or `DECIMAL(10, 4)`, etc.

##### DecimalType

| Field Name | Field Type | Description        | Optional |
|------------|------------|--------------------| -------- |
| name       | string     | Type Name.         | Required |
| precision  | uint32     | Decimal precision. | Required |
| scale      | uint32     | Decimal scale.     | Required |

##### VarcharType

| Field Name | Field Type | Description         | Optional |
|------------|------------|---------------------| -------- |
| name       | string     | Type Name.          | Required |
| length     | uint32     | varchar's length.   | Required |

##### ListType

| Field Name   | Field Type | Description                              | Optional |
|--------------|-------|------------------------------------------| -------- |
| name         | string | Type Name.                               | Required |
| element_type | TypeCategory     | list element type category.              | Required |

##### MapType

| Field Name | Field Type | Description                   | Optional |
|------------|-------|-------------------------------| -------- |
| name       | string | Type Name.                    | Required |
| key_type   | TypeCategory     | Map's key type category.      | Required |
| value_type | TypeCategory     | Map's value type category. | Required |

##### VarbinaryType

| Field Name | Field Type | Description           | Optional |
|------------|------------|-----------------------| -------- |
| name       | string     | Type Name.            | Required |
| length     | uint32     | binary's byte length. | Required |

### Type Conversions

Each data source has its own type of definite. There are many different between them and `Graviton`,
In order to simplify type conversions, We support mapping the relationship between the two via configure.

The connector uses `TypeConverter` object to load the configuration YAML file of the type converter,
basis on the name of the data source, Converts the filed types of the data source to `Gravition` Unified field type.

The rule of the `Graviton` type converter configure file is `{data-source-name}-{version}-type-converter.yaml`,   
Each data source has a `default` version as the base configure file.

When there is a difference between the new version of the data source field type and the default version,
We can replace the default version with the higher version.

```bash
hive-default-type-converter.yaml
hive-3.1.3-type-converter.yaml
```

The format of the type converter configure file like this
```bash
# Hive Type to Graviton Type
datasource.type.to.graviton.type.converter:
  - source.type: TINYINT
    target.type: Int8

  - source.type: SMALLINT
    target.type: Int16

  - source.type: INT
    target.type: Int32

  - source.type: BIGINT
    target.type: Int64

  - source.type: null
    target.type: void
```

### Metadata multiple version

When the connector discovers that the metadata has changed, The connector will create a new version and timestamp
of the metadata.
This allow us to track the history of metadata changes and store all historical metadata information.
Use can view and rollback to any version of metadata.

### Metadata operation

The Connector Metadata operation readonly interface allows `Graviton` to obtain a list of lakehouse, tenant, 
zone, schemas, tables, view and columns and other metadata for a specific data source.

+ listLakehouseNames
+ listTenantNames
+ listZoneNames
+ listTableNames
+ listViewNames
+ listPartitionNames
+ listColumnNames

The connector metadata interface allows to implement other write operation features, like:
+ Schema manager, which is creating, altering and dropping lakehouse, tenant, zone, schemas, tables, Materialized views and columns.
+ Support for lakehouse, tenant, zone, schemas, tables, mview and columns comments and properties.
+ Support for lakehouse statistics. (Follow up to add)
+ Support for tenant statistics. (Follow up to add)
+ Support for zone statistics. (Follow up to add)
+ Support for schemas statistics. (Follow up to add)
+ Support for tables, mview statistics, including total number of rows and `columns statistics` etc.
+ Support `columns statistics`, including MIN and MAX values, ranger of values, number of the distinct values etc.

### System Tables

`Graviton` allows manipulation of metadata through SQL in addition to providing a RESTful API.

`Graviton`'s metadata is stored in the back-end storage layer, They are abstracted as a set of system tables,
different users can access different data base on authorization.

You can use SQL to operation `Graviton` system metadata, e.g:
+ If you execute `SELECT * FROM system.lakehouse WEHRE name = 'foo'` statements, then return lakehouse's all information,
such as name and properties.
