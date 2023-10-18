---
title: "Gravitino connector"
date: 2023-10-01T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---
# Gravitino connector

The Gravitino connector allows query tables manager by gravitino. 
This can be used to manager catalogs, schemas, and tables and dynamically load them into trino. 
And query data in the original databases on the server, 
or combine it with other data from different catalogs accessing any other supported data source.

### Supported catalog types
The Graviton connector supports various Trino connector types, including:
- [Hive catalog]()

# Requirements

To connect to a Gravitino server, you need:
- Trino server version is higher than Trino-server-360.
- Network access from the Trino coordinator and workers to the Gravitino server. Port 8090 is the default port.
- Trino needs to install connectors for all the Gravitino-manged types fo catalogs.

# Installation

To install the Gravitino connector, you should deploy the Trino environment first.
Then install the Gravitino connector plugin to Trino.
Please refer to the [Deploying Trino documentation](https://trino.io/docs/current/installation/deployment.html).
1. Download Gravitino connector tarball, [gravitino-connector-0.2.tar.gz](), and unpack it. The tarball contains a single 
   top-level directory `gravitino-connector`, which we call the connector directory. 
2. Copy the connector directory to trino plugin's directory.
   Normally, the directory location is `Trino-server-xxx/plugin`, and the directory contains another catalog used by Trino.
3. Add jvm arguments `-Dlog4j.configurationFile=file:///xxx/log4j2.properties` to enable logging of Gravitino connector.

Alternatively,
you can build the Gravitino connector package from sources
and obtain the `gravitino-connector-0.2.tar.gz` file in the `$PROJECT/distribute/trino-connector` directory.
Please refer to the [Gravitino Development documentation]()

# Configuration

The connector can access a Gravitino server. Create a catalog properties file that specifies the Gravitino connector by setting the 
`connector.name` to `gravitino`.

For example, create the file etc/catalog/gravitino.properties. Replace the connection properties as appropriate for your setup:

```
connector.name=gravitino
gravitino.uri=http://host:8090
gravitino.metalake=test
```
The `gravitino.uri` defines the connection information about Gravitino server.  
The `gravitino.metalake` defines which metalake are used.

## Multiple Gravitino metalakes
If you have multiple Gravitino metalakes, you need to configure one catalog for each metalake. To add another catalog:
- Add another properties file to `etc/catalog`
- Save it with a different name that ends in `.properties`

For example, if you name the property file sales.properties. Trino uses the configured connector to create a catalog named `sales`

# SQL support
The connector provides read access and write access to data and metadata by the type fo gravitino catalog. 

## Basic usage examples
First, you need to create a metalake and catalog in Gravitino.
For example, create a new metalake named `test` and create a new catalog named `hive_text` using the hive provider. [Gravitino catalog docs]()

Listing all Gravitino manager catalogs
```
show catalogs
```
The results like:
```
    Catalog
----------------
 gravitino
 jmx
 system
 test.hive_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravtion` catalog is the config catalog by the give metalake. We generally do not use it.
The `test.hive_test` catalog is Gravitino managed catalog. It dynamically loaded by the `gravtion` catalog from Gravitino. It is a Hive catalog.
Other catalogs are user-configured regular Trino catalogs.

Create a new schema named `database_01` in `test.hive_test` catalog.
```
create schema "test.hive_test".database_01;
```

Create a new table named `table_01` in `"test.hive_test".database_01` schema. That is stored using the TEXTFILE format.
```
create table  "test.hive_test".database_01.table_01
(
name varchar,
salary int
)
WITH (
  format = 'TEXTFILE'
);
```

Drop a schema:
```
drop schema "test.hive_test".database_01;
```

Drop a schema:
```
drop table "test.hive_test".database_01.table_01;
```

Query the `table_01` table: 
```
select * from "test.hive_test".database_01.table_01;
```

Insert data to the table `table_01`:  
```
insert into  "test.hive_test".database_01.table_01 (name, salary) values ('ice', 12);
```





