---
title: "Command Line Interface"
slug: "/cli"
keyword: "cli"
last_update:
  date: 2024-10-23
  author: justinmclean
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This document provides guidance on managing metadata within Apache Gravitino using the Command Line Interface (CLI). The CLI offers a terminal based alternative to using code or the REST interface for metadata management.

The CLI allows users to view, create and update metadata information for metalakes, catalogs, schemas, tables, models, columns, users, roles, groups, tags, topics and filesets. Future updates will expand on these capabilities.

## Run the CLI

To run the Gravitino CLI, use the `gcli.sh` script in Gravitino's bin directory. If you define the `GRAVITINO_HOME` environment variable, you can run the script from any location using `$GRAVITINO_HOME/bin/gcli.sh`.

## Usage

The general structure for running commands with the Gravitino CLI is `gcli.sh entity command [options]`.

 ```bash
  usage: gcli.sh [metalake|catalog|schema|model|table|column|user|group|tag|topic|fileset] [list|details|create|delete|update|set|remove|properties|revoke|grant] [options]
  Options
 usage: gcli
 -a,--audit              display audit information
    --alias <arg>        model aliases
    --all                on all entities
    --auto <arg>         column value auto-increments (true/false)
 -c,--comment <arg>      entity comment
    --columnfile <arg>   CSV file describing columns
 -d,--distribution       display distribution information
    --datatype <arg>     column data type
    --default <arg>      default column value
    --disable            disable entities
    --enable             enable entities
 -f,--force              force operation
 -g,--group <arg>        group name
 -h,--help               command help information
 -i,--ignore             ignore client/sever version check
 -l,--user <arg>         user name
    --login <arg>        user name
 -m,--metalake <arg>     metalake name
 -n,--name <arg>         full entity name (dot separated)
    --null <arg>         column value can be null (true/false)
 -o,--owner              display entity owner
    --output <arg>       output format (plain/table)
 -P,--property <arg>     property name
 -p,--properties <arg>   property name/value pairs
    --partition          display partition information
    --position <arg>     position of column
    --privilege <arg>    privilege(s)
    --quiet              quiet mode
 -r,--role <arg>         role name
    --rename <arg>       new entity name
 -s,--server             Gravitino server version
    --simple             simple authentication
    --sortorder          display sortorder information
 -t,--tag <arg>          tag name
 -u,--url <arg>          Gravitino URL (default: http://localhost:8090)
    --uris <arg>         model version artifact
 -v,--version            Gravitino client version
 -V,--value <arg>        property value
 -x,--index              display index information
 -z,--provider <arg>     provider one of hadoop, hive, mysql, postgres,
                         iceberg, kafka
 ```

## Commands

The following commands are used for entity management:

- list: List available entities
- details: Show detailed information about an entity
- create: Create a new entity
- delete: Delete an existing entity
- update: Update an existing entity
- set: Set a property on an entity
- remove: Remove a property from an entity
- properties: Display an entities properties

### Set the Metalake Name

As dealing with one Metalake is a typical scenario, you can set the Metalake name in several ways so it doesn't need to be passed on the command line.

1. Passed in on the command line via the `--metalake` parameter.
2. Set via the `GRAVITINO_METALAKE` environment variable.
3. Stored in the Gravitino CLI configuration file.

The command line option overrides the environment variable and the environment variable overrides the configuration file.

### Set the Gravitino URL

As you need to set the Gravitino URL for every command, you can set the URL in several ways:

1. Passed in on the command line via the `--url` parameter.
2. Set via the 'GRAVITINO_URL' environment variable.
3. Stored in the Gravitino CLI configuration file.

The command line option overrides the environment variable and the environment variable overrides the configuration file.

### Set the Authentication Type

The authentication type can also be set in several ways:

1. Passed in on the command line via the `--simple` flag.
2. Set via the 'GRAVITINO_AUTH' environment variable.
3. Stored in the Gravitino CLI configuration file.

### CLI Configuration File

The gravitino CLI can read commonly used CLI options from a configuration file. By default, the file is `.gravitino` in the user's home directory. The metalake, URL and ignore parameters can be set in this file.

```text
#
# Gravitino CLI configuration file
#

# Metalake to use
metalake=metalake_demo

# Gravitino server to connect to
URL=http://localhost:8090

# Ignore client/server version mismatch
ignore=true

# Authentication
auth=simple

```

OAuth authentication can also be configured via the configuration file.

```text
# Authentication
auth=oauth
serverURI=http://127.0.0.1:1082
credential=xx:xx
token=test
scope=token/test
```

Kerberos authentication can also be configured via the configuration file.

```text
# Authentication
auth=kerberos
principal=user/admin@foo.com
keytabFile=file.keytab
```

### Potentially Unsafe Operations

For operations that delete data or rename a metalake the user with be prompted to make sure they wish to run this command. The `--force` option can be specified to override this behaviour.

### Manage Metadata

All the commands are performed by using the [Java API](api/java-api) internally.

### Display Help

To display help on command usage:

```bash
gcli.sh --help
```

### Display Client Version

To display the client version:

```bash
gcli.sh --version
```

### Display Server Version

To display the server version:

```bash
gcli.sh --server
```

### Client/Server Version Mismatch

If the client and server are running different versions of the Gravitino software then you may need to ignore the client/server version check for the command to run. This can be done in several ways:

1. Passed in on the command line via the `--ignore` parameter.
2. Set via the `GRAVITINO_IGNORE` environment variable.
3. Stored in the Gravitino CLI configuration file.

### Multiple Properties

For commands that accept multiple properties they can be specified in a couple of different ways:

1. gcli.sh --properties n1=v1,n2=v2,n3=v3

2. gcli.sh --properties n1=v1 n2=v2 n3=v3

3. gcli.sh --properties n1=v1 --properties n2=v2 --properties n3=v3

### Set Properties and Tags

 Different options are needed to add a tag and set a property of a tag with `gcli.sh tag set`. To add a
 tag, specify the tag (via --tag) and the entity to tag (via --name). To set the property of a tag
 (via --tag) you need to specify the property (via --property) and value (via --value) you want to
 set.

 To delete a tag, again, you need to specify the tag and entity, to remove a tag's property you need
 to select the tag and property.

### CLI Commands

Please set the metalake in the Gravitino configuration file or the environment variable before running any of these commands.

### Metalake Commands

#### Show All Metalakes

```bash
gcli.sh metalake list
```

#### Metalake Details

```bash
gcli.sh metalake details
```

#### Metalake Audit Information

```bash
gcli.sh metalake details --audit
```

#### Create a Metalake

```bash
gcli.sh metalake create --metalake my_metalake --comment "This is my metalake"
```

#### Delete a Metalake

```bash
gcli.sh metalake delete
```

#### Rename a Metalake

```bash
gcli.sh metalake update --rename demo
```

#### Update a Metalake Comment

```bash
gcli.sh metalake update --comment "new comment"
```

#### Metalake Properties

```bash
gcli.sh metalake properties
```

#### Set a Metalake Property

```bash
gcli.sh metalake set --property test --value value
```

#### Remove a Metalake Property

```bash
gcli.sh metalake remove --property test
```

#### Enable a Metalake

```bash
gcli.sh metalake update -m metalake_demo --enable
```

#### Enable a Metalake and All Catalogs

```bash
gcli.sh metalake update -m metalake_demo --enable --all
```

#### Disable a Metalake

```bash
gcli.sh metalake update -m metalake_demo --disable
```

### Catalog Commands

#### Show All Catalogs in a Metalake

```bash
gcli.sh catalog list
```

#### Catalog Details

```bash
gcli.sh catalog details --name catalog_postgres
```

#### Catalog Audit Information

```bash
gcli.sh catalog details --name catalog_postgres --audit
```

#### Create a Catalog

The type of catalog to be created is specified by the `--provider` option. Different catalogs require different properties, for example, a Hive catalog requires a metastore-uri property.

##### Create a Hive catalog

```bash
gcli.sh catalog create --name hive --provider hive --properties metastore.uris=thrift://hive-host:9083
```

##### Create an Iceberg catalog

```bash
gcli.sh catalog create  -name iceberg --provider iceberg --properties uri=thrift://hive-host:9083,catalog-backend=hive,warehouse=hdfs://hdfs-host:9000/user/iceberg/warehouse
```

##### Create a MySQL catalog

```bash
gcli.sh catalog create  -name mysql --provider mysql --properties jdbc-url=jdbc:mysql://mysql-host:3306?useSSL=false,jdbc-user=user,jdbc-password=password,jdbc-driver=com.mysql.cj.jdbc.Driver
```

##### Create a Postgres catalog

```bash
gcli.sh catalog create  -name postgres --provider postgres --properties jdbc-url=jdbc:postgresql://postgresql-host/mydb,jdbc-user=user,jdbc-password=password,jdbc-database=db,jdbc-driver=org.postgresql.Driver
```

##### Create a Kafka catalog

```bash
gcli.sh catalog create --name kafka --provider kafka --properties bootstrap.servers=127.0.0.1:9092,127.0.0.2:9092
```

##### Create a Doris catalog

```bash
gcli.sh catalog create --name doris --provider doris --properties jdbc-url=jdbc:mysql://localhost:9030,jdbc-driver=com.mysql.jdbc.Driver,jdbc-user=admin,jdbc-password=password
```

##### Create a Paimon catalog

```bash
gcli.sh catalog create --name paimon --provider paimon --properties catalog-backend=jdbc,uri=jdbc:mysql://127.0.0.1:3306/metastore_db,authentication.type=simple
```

##### Create a Hudi catalog

```bash
gcli.sh catalog create --name hudi --provider hudi --properties catalog-backend=hms,uri=thrift://127.0.0.1:9083
```

##### Create an OceanBase catalog

```bash
gcli.sh catalog create --name oceanbase --provider oceanbase --properties jdbc-url=jdbc:mysql://localhost:2881,jdbc-driver=com.mysql.jdbc.Driver,jdbc-user=admin,jdbc-password=password
```

#### Delete a Catalog

```bash
gcli.sh catalog delete --name hive
```

#### Rename a Catalog

```bash
gcli.sh catalog update --name catalog_mysql --rename mysql
```

#### Change a Catalog Comment

```bash
gcli.sh catalog update --name catalog_mysql --comment "new comment"
```

#### Catalog Properties

```bash
gcli.sh catalog properties --name catalog_mysql
```

#### Set a Catalog Property

```bash
gcli.sh catalog set --name catalog_mysql --property test --value value
```

#### Remove a Catalog Property

```bash
gcli.sh catalog remove --name catalog_mysql --property test
```

#### Enable a Catalog

```bash
gcli.sh catalog update -m metalake_demo --name catalog --enable 
```

#### Enable a Catalog and Its Metalake

```bash
gcli.sh catalog update -m metalake_demo --name catalog --enable --all
```

#### Disable a Catalog

```bash
gcli.sh catalog update -m metalake_demo --name catalog --disable
```

### Schema Commands

#### Show All Schemas in a Catalog

```bash
gcli.sh schema list --name catalog_postgres
```

#### Schema Details

```bash
gcli.sh schema details --name catalog_postgres.hr
```

#### Schema Audit Information

```bash
gcli.sh schema details --name catalog_postgres.hr --audit
```

#### Create a Schema

```bash
gcli.sh schema create --name catalog_postgres.new_db
```

#### Schema Properties

```bash
gcli.sh schema properties --name catalog_postgres.hr
```

Setting and removing schema properties is not supported by the Java API or the Gravitino CLI.

### Table Commands

When creating a table the columns are specified in CSV file specifying the name of the column, the datatype, a comment, true or false if the column is nullable, true or false if the column is auto incremented, a default value and a default type. Not all of the columns need to be specified just the name and datatype columns. If not specified comment default to null, nullability to true and auto increment to false. If only the default value is specified it defaults to the same data type as the column.

Example CSV file

```text
Name,Datatype,Comment,Nullable,AutoIncrement,DefaultValue,DefaultType
name,String,person's name
ID,Integer,unique id,false,true
location,String,city they work in,false,false,Sydney,String
```

#### Show All Tables

```bash
gcli.sh table list --name catalog_postgres.hr
```

#### Table Details

```bash
gcli.sh table details --name catalog_postgres.hr.departments
```

#### Table Audit Information

```bash
gcli.sh table details --name catalog_postgres.hr.departments --audit
```

#### Table Distribution Information

```bash
gcli.sh table details --name catalog_postgres.hr.departments --distribution
```

#### Table Partition Information

```bash
gcli.sh table details --name catalog_postgres.hr.departments --partition
```

#### Table Sort Order Information

```bash
gcli.sh table details --name catalog_postgres.hr.departments --sortorder
```

### Table Indexes

```bash
gcli.sh table details --name catalog_mysql.db.iceberg_namespace_properties --index
```

#### Delete a Table

```bash
gcli.sh table delete --name catalog_postgres.hr.salaries
```

#### Table Properties

```bash
gcli.sh table properties --name catalog_postgres.hr.salaries
```

#### Set a Table Property

```bash
gcli.sh table set --name catalog_postgres.hr.salaries --property test --value value
```

#### Remove a Table Property

```bash
gcli.sh table remove --name catalog_postgres.hr.salaries --property test
```

#### Create a Table

```bash
gcli.sh table create --name catalog_postgres.hr.salaries --comment "comment" --columnfile ~/table.csv
```

### User Commands

#### Create a User

```bash
gcli.sh user create --user new_user
```

#### User Details

```bash
gcli.sh user details --user new_user
```

#### List All Users

```bash
gcli.sh user list
```

#### Role Audit Information

```bash
gcli.sh user details --user new_user --audit
```

#### Delete a User

```bash
gcli.sh user delete --user new_user
```

### Group Commands

#### Create a Group

```bash
gcli.sh group create --group new_group
```

#### Group Details

```bash
gcli.sh group details --group new_group
```

#### List All Groups

```bash
gcli.sh group list
```

#### Group Audit Information

```bash
gcli.sh group details --group new_group --audit
```

#### Delete a Group

```bash
gcli.sh group delete --group new_group
```

### Tag Commands

#### Tag Details

```bash
gcli.sh tag details --tag tagA
```

#### Create Tags

```bash
gcli.sh tag create --tag tagA tagB
```

#### List All Tags

```bash
gcli.sh tag list
```

#### Delete Tags

```bash
gcli.sh tag delete --tag tagA tagB
```

#### Add Tags to an Entity

```bash
gcli.sh tag set --name catalog_postgres.hr --tag tagA tagB
```

#### Remove Tags from an Entity

```bash
gcli.sh tag remove --name catalog_postgres.hr --tag tagA tagB
```

#### Remove All Tags from an Entity

```bash
gcli.sh tag remove --name catalog_postgres.hr
```

#### List All Tags on an Entity

```bash
gcli.sh tag list --name catalog_postgres.hr
```

#### List the Properties of a Tag

```bash
gcli.sh tag properties --tag tagA
```

#### Set a Property of a Tag

```bash
gcli.sh tag set --tag tagA --property test --value value
```

#### Delete a Property of a Tag

```bash
gcli.sh tag remove --tag tagA --property test
```

#### Rename a Tag

```bash
gcli.sh tag update --tag tagA --rename newTag
```

#### Update a Tag Comment

```bash
gcli.sh tag update --tag tagA --comment "new comment"
```

### Owner Commands

#### List an Owner

```bash
gcli.sh catalog details --owner --name postgres
```

#### Set an Owner to a User

```bash
gcli.sh catalog set --owner --user admin --name postgres
```

#### Set an Owner to a Group

```bash
gcli.sh catalog set --owner --group groupA --name postgres
```

### Role Commands

When granting or revoking privileges the following privileges can be used.

create_catalog, use_catalog, create_schema, use_schema, create_table, modify_table, select_table, create_fileset, write_fileset, read_fileset, create_topic, produce_topic, consume_topic, manage_users, create_role, manage_grants

Note that some are only valid for certain entities.

#### Role Details

```bash
gcli.sh role details --role admin
```

#### List All Roles

```bash
gcli.sh role list
```

#### Role Audit Information

```bash
gcli.sh role details --role admin --audit
```

#### Create a Role

```bash
gcli.sh role create --role admin
```

#### Delete a Role

```bash
gcli.sh role delete --role admin
```

#### Add a Role to a User

```bash
gcli.sh user grant --user new_user --role admin
```

#### Remove a Role from a User

```bash
gcli.sh user revoke --user new_user --role admin
```

#### Remove All Roles from a User

```bash
gcli.sh user revoke --user new_user --all
```

#### Add a Role to a Group

```bash
gcli.sh group grant --group groupA --role admin
```

#### Remove a Role from a Group

```bash
gcli.sh group revoke --group groupA --role admin
```

#### Remove All Roles from a Group

```bash
gcli.sh group revoke --group groupA --all
```

### Privilege Commands

#### Grant a Privilege

```bash
gcli.sh role grant --name catalog_postgres --role admin --privilege create_table modify_table
```

#### Revoke a Privilege

```bash
gcli.sh role revoke --metalake metalake_demo --name catalog_postgres --role admin --privilege create_table modify_table
```

#### Revoke All Privileges

```bash
gcli.sh role revoke --metalake metalake_demo --name catalog_postgres --role admin --all
```

### Topic Commands

#### Topic Details

```bash
gcli.sh topic details --name kafka.default.topic3
```

#### Create a Topic

```bash
gcli.sh topic create --name kafka.default.topic3
```

#### List All Topics

```bash
gcli.sh topic list --name kafka.default
```

#### Delete a Topic

```bash
gcli.sh topic delete --name kafka.default.topic3
```

#### Change a Topic Comment

```bash
gcli.sh topic update --name kafka.default.topic3 --comment new_comment
```

#### Topic Properties

```bash
gcli.sh topic properties --name kafka.default.topic3
```

#### Set a Topic Property

```bash
gcli.sh topic set --name kafka.default.topic3 --property test --value value
```

#### Remove a Topic Property

```bash
gcli.sh topic remove --name kafka.default.topic3 --property test
```

### Fileset Commands

#### Create a Fileset

```bash
gcli.sh fileset create --name hadoop.schema.fileset --properties managed=true,location=file:/tmp/root/schema/example
```

#### List Filesets

```bash
gcli.sh fileset list --name hadoop.schema
```

#### Fileset Details

```bash
gcli.sh fileset details --name hadoop.schema.fileset
```

#### Delete a Fileset

```bash
gcli.sh fileset delete --name hadoop.schema.fileset
```

#### Update a Fileset Comment

```bash
gcli.sh fileset update --name hadoop.schema.fileset --comment new_comment
```

#### Rename a Fileset

```bash
gcli.sh fileset update --name hadoop.schema.fileset --rename new_name
```

#### Fileset Properties

```bash
gcli.sh fileset properties --name hadoop.schema.fileset 
```

#### Set a Fileset Property

```bash
gcli.sh fileset set  --name hadoop.schema.fileset --property test --value value
```

#### Remove a Fileset Property

```bash
gcli.sh fileset remove --name hadoop.schema.fileset --property test
```

### Column Commands

Note that some commands are not supported depending on what the database supports.

When setting the datatype of a column the following basic types are supported:
null, boolean, byte, ubyte, short, ushort, integer, uinteger, long, ulong, float, double, date, time, timestamp, tztimestamp, intervalyear, intervalday, uuid, string, binary

In addition decimal(precision,scale), fixed(length), fixedchar(length) and varchar(length).

#### Show All Columns

```bash
gcli.sh column list --name catalog_postgres.hr.departments
```

#### Column Audit Information

```bash
gcli.sh column details --name catalog_postgres.hr.departments.name --audit
```

#### Add a Column

```bash
gcli.sh column create --name catalog_postgres.hr.departments.value --datatype long
gcli.sh column create --name catalog_postgres.hr.departments.money --datatype "decimal(10,2)"
gcli.sh column create --name catalog_postgres.hr.departments.name --datatype "varchar(100)"
gcli.sh column create --name catalog_postgres.hr.departments.fullname --datatype "varchar(250)" --default "Fred Smith" --null=false
```

#### Delete a Column

```bash
gcli.sh  column delete --name catalog_postgres.hr.departments.money
```

#### Update a Column

```bash
gcli.sh column update --name catalog_postgres.hr.departments.value --rename values
gcli.sh column update --name catalog_postgres.hr.departments.values --datatype "varchar(500)"
gcli.sh column update --name catalog_postgres.hr.departments.values --position name
gcli.sh column update --name catalog_postgres.hr.departments.name --null true
```

#### Simple Authentication

```bash
gcli.sh <normal command> --simple
```

### Authentication

#### Simple Authentication with User Name

```bash
gcli.sh <normal command> --simple --login userName
```

### Model Commands

#### Create a Model

```bash
gcli.sh model create --name catalog_model.schema.model
```

#### List Models

```bash
gcli.sh model list --name catalog_model.schema
```

#### Model Details

```bash
gcli.sh model details --name catalog_model.schema.model
```

#### Model Audit Information

```bash
gcli.sh model details --name catalog_model.schema.model --audit
```

#### Delete a Model

```bash
gcli.sh model delete --name catalog_model.schema.model
```

#### Update a Model Comment

```bash
gcli.sh model update --name catalog_model.schema.model --comment new_comment
```

#### Rename a Model

```bash
gcli.sh model update --name catalog_model.schema.model --rename new_name
```

#### Set a Model Property

```bash
gcli.sh model set --name catalog_model.schema.model --property k --value v
```

#### Remove a Model Property

```bash
gcli.sh model remove --name catalog_model.schema.model --property k
```

#### Link a Model Version

```bash
gcli.sh model update --name catalog_model.schema.model --uris s3=s3://path,hdfs=hdfs://path --alias alias1
```

#### Update a Model Version Comment

```bash
gcli.sh model update --name catalog_model.schema.model --version 0 --comment new_comment
```

#### Add Aliases to a Model Version

```bash
gcli.sh model update --name catalog_model.schema.model --version 0 --newalias alias1
```

#### Remove Aliases of a Model Version

```bash
gcli.sh model remove --name catalog_model.schema.model --version 0 --removealias alias1
```

#### Set a Model Version Property

```bash
gcli.sh model set --name catalog_model.schema.model --version 0 --property k --value v
```

#### Remove a Model Version Property

```bash
gcli.sh model remove --name catalog_model.schema.model --version 0 --property k
```

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=CLI" alt="" />
