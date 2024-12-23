---
title: 'Apache Gravitino Command Line Interface'
slug: /cli
keyword: cli
last_update:
  date: 2024-10-23
  author: justinmclean
license: 'This software is licensed under the Apache License version 2.'
---

This document provides guidance on managing metadata within Apache Gravitino using the Command Line Interface (CLI). The CLI offers a terminal based alternative to using code or the REST interface for metadata management.

Currently, the CLI allows users to view metadata information for metalakes, catalogs, schemas, tables, users, roles, groups, tags, topics and filesets. Future updates will expand on these capabilities.

## Running the CLI

You can configure an alias for the CLI for ease of use, with the following command:

```bash
alias gcli='java -jar ../../cli/build/libs/gravitino-cli-*-incubating-SNAPSHOT.jar'
```

Or you use the `gcli.sh` script found in the `clients/cli/bin/` directory to run the CLI.

## Usage

The general structure for running commands with the Gravitino CLI is `gcli entity command [options]`.

 ```bash
  usage: gcli [metalake|catalog|schema|table|column|user|group|tag|topic|fileset] [list|details|create|delete|update|set|remove|properties|revoke|grant] [options]
  Options
 usage: gcli
 -a,--audit              display audit information
    --auto <arg>         column value auto-increments (true/false)
 -c,--comment <arg>      entity comment
    --columnfile <arg>   CSV file describing columns
 -d,--distribution       display distribution information
    --datatype <arg>     column data type
    --default <arg>      default column value
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
 -r,--role <arg>         role name
    --rename <arg>       new entity name
 -s,--server             Gravitino server version
    --simple             simple authentication
    --sortorder          display sortorder information
 -t,--tag <arg>          tag name
 -u,--url <arg>          Gravitino URL (default: http://localhost:8090)
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

### Setting the Metalake name

As dealing with one Metalake is a typical scenario, you can set the Metalake name in several ways so it doesn't need to be passed on the command line.

1. Passed in on the command line via the `--metalake` parameter.
2. Set via the `GRAVITINO_METALAKE` environment variable.
3. Stored in the Gravitino CLI configuration file.

The command line option overrides the environment variable and the environment variable overrides the configuration file.

### Setting the Gravitino URL

As you need to set the Gravitino URL for every command, you can set the URL in several ways.

1. Passed in on the command line via the `--url` parameter.
2. Set via the 'GRAVITINO_URL' environment variable.
3. Stored in the Gravitino CLI configuration file.

The command line option overrides the environment variable and the environment variable overrides the configuration file.

### Setting the Gravitino Authentication Type

The authentication type can also be set in several ways.

1. Passed in on the command line via the `--simple` flag.
2. Set via the 'GRAVITINO_AUTH' environment variable.
3. Stored in the Gravitino CLI configuration file.

### Gravitino CLI configuration file

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

### Potentially unsafe operations

For operations that delete data or rename a metalake the user with be prompted to make sure they wish to run this command. The `--force` option can be specified to override this behaviour.

### Manage metadata

All the commands are performed by using the [Java API](api/java-api) internally.

### Display help

To display help on command usage:

```bash
gcli --help
```

### Display client version

To display the client version:

```bash
gcli --version
```

### Display server version

To display the server version:

```bash
gcli --server
```

### Client/server version mismatch

If the client and server are running different versions of the Gravitino software then you may need to ignore the client/server version check for the command to run. This can be done in several ways:

1. Passed in on the command line via the `--ignore` parameter.
2. Set via the `GRAVITINO_IGNORE` environment variable.
3. Stored in the Gravitino CLI configuration file.

### Multiple properties

For commands that accept multiple properties they can be specified in a couple of different ways:

1. gcli --properties n1=v1,n2=v2,n3=v3

2. gcli --properties n1=v1 n2=v2 n3=v3

3. gcli --properties n1=v1 --properties n2=v2 --properties n3=v3

### Setting properties and tags

 Different options are needed to add a tag and set a property of a tag with `gcli tag set`. To add a
 tag, specify the tag (via --tag) and the entity to tag (via --name). To set the property of a tag
 (via --tag) you need to specify the property (via --property) and value (via --value) you want to
 set.

 To delete a tag, again, you need to specify the tag and entity, to remove a tag's property you need
 to select the tag and property.

### CLI commands

Please set the metalake in the Gravitino configuration file or the environment variable before running any of these commands.

### Metalake commands

#### Show all metalakes

```bash
gcli metalake list
```

#### Show a metalake details

```bash
gcli metalake details
```

#### Show a metalake's audit information

```bash
gcli metalake details --audit
```

#### Create a metalake

```bash
gcli metalake create --metalake my_metalake --comment "This is my metalake"
```

#### Delete a metalake

```bash
gcli metalake delete
```

#### Rename a metalake

```bash
gcli metalake update --rename demo
```

#### Update a metalake's comment

```bash
gcli metalake update --comment "new comment"
```

#### Display a metalake's properties

```bash
gcli metalake properties
```

#### Set a metalake's property

```bash
gcli metalake set --property test --value value
```

#### Remove a metalake's property

```bash
gcli metalake remove --property test
```

### Catalog commands

#### Show all catalogs in a metalake

```bash
gcli catalog list
```

#### Show a catalog details

```bash
gcli catalog details --name catalog_postgres
```

#### Show a catalog's audit information

```bash
gcli catalog details --name catalog_postgres --audit
```

#### Creating a catalog

The type of catalog to be created is specified by the `--provider` option. Different catalogs require different properties, for example, a Hive catalog requires a metastore-uri property.

##### Create a Hive catalog

```bash
gcli catalog create --name hive --provider hive --properties metastore.uris=thrift://hive-host:9083
```

##### Create an Iceberg catalog

```bash
gcli catalog create  -name iceberg --provider iceberg --properties uri=thrift://hive-host:9083,catalog-backend=hive,warehouse=hdfs://hdfs-host:9000/user/iceberg/warehouse
```

##### Create a MySQL catalog

```bash
gcli catalog create  -name mysql --provider mysql --properties jdbc-url=jdbc:mysql://mysql-host:3306?useSSL=false,jdbc-user=user,jdbc-password=password,jdbc-driver=com.mysql.cj.jdbc.Driver
```

##### Create a Postgres catalog

```bash
gcli catalog create  -name postgres --provider postgres --properties jdbc-url=jdbc:postgresql://postgresql-host/mydb,jdbc-user=user,jdbc-password=password,jdbc-database=db,jdbc-driver=org.postgresql.Driver
```

##### Create a Kafka catalog

```bash
gcli catalog create --name kafka --provider kafka --properties bootstrap.servers=127.0.0.1:9092,127.0.0.2:9092
```

##### Create a Doris catalog

```bash
gcli catalog create --name doris --provider doris --properties jdbc-url=jdbc:mysql://localhost:9030,jdbc-driver=com.mysql.jdbc.Driver,jdbc-user=admin,jdbc-password=password
```

##### Create a Paimon catalog

```bash
gcli catalog create --name paimon --provider paimon --properties catalog-backend=jdbc,uri=jdbc:mysql://127.0.0.1:3306/metastore_db,authentication.type=simple
```

#### Create a Hudi catalog

```bash
gcli catalog create --name hudi --provider hudi --properties catalog-backend=hms,uri=thrift://127.0.0.1:9083
```

#### Create an Oceanbase catalog

```bash
gcli catalog create --name oceanbase --provider oceanbase --properties jdbc-url=jdbc:mysql://localhost:2881,jdbc-driver=com.mysql.jdbc.Driver,jdbc-user=admin,jdbc-password=password
```

#### Delete a catalog

```bash
gcli catalog delete --name hive
```

#### Rename a catalog

```bash
gcli catalog update --name catalog_mysql --rename mysql
```

#### Change a catalog comment

```bash
gcli catalog update --name catalog_mysql --comment "new comment"
```

#### Display a catalog's properties

```bash
gcli catalog properties --name catalog_mysql
```

#### Set a catalog's property

```bash
gcli catalog set --name catalog_mysql --property test --value value
```

#### Remove a catalog's property

```bash
gcli catalog remove --name catalog_mysql --property test
```

### Schema commands

#### Show all schemas in a catalog

```bash
gcli schema list --name catalog_postgres
```

#### Show schema details

```bash
gcli schema details --name catalog_postgres.hr
```

#### Show schema's audit information

```bash
gcli schema details --name catalog_postgres.hr --audit
```

#### Create a schema

```bash
gcli schema create --name catalog_postgres.new_db
```

#### Display schema properties

```bash
gcli schema properties --name catalog_postgres.hr
```

Setting and removing schema properties is not currently supported by the Java API or the Gravitino CLI.

### Table commands

When creating a table the columns are specified in CSV file specifying the name of the column, the datatype, a comment, true or false if the column is nullable, true or false if the column is auto incremented, a default value and a default type. Not all of the columns need to be specifed just the name and datatype columns. If not specified comment default to null, nullability to true and auto increment to false. If only the default value is specified it defaults to the same data type as the column.

Example CSV file
```text
Name,Datatype,Comment,Nullable,AutoIncrement,DefaultValue,DefaultType
name,String,person's name
ID,Integer,unique id,false,true
location,String,city they work in,false,false,Sydney,String
```

#### Show all tables

```bash
gcli table list --name catalog_postgres.hr
```

#### Show tables details

```bash
gcli table details --name catalog_postgres.hr.departments
```

#### Show tables audit information

```bash
gcli table details --name catalog_postgres.hr.departments --audit
```

#### Show tables distribution information
```bash
gcli table details --name catalog_postgres.hr.departments --distribution
```

#### Show tables partition information
```bash
gcli table details --name catalog_postgres.hr.departments --partition
```

#### Show tables sort order information
```bash
gcli table details --name catalog_postgres.hr.departments --sortorder
```

### Show table indexes

```bash
gcli table details --name catalog_mysql.db.iceberg_namespace_properties --index
```

#### Delete a table

```bash
gcli table delete --name catalog_postgres.hr.salaries
```


#### Display a tables's properties

```bash
gcli table properties --name catalog_postgres.hr.salaries
```

#### Set a tables's property

```bash
gcli table set --name catalog_postgres.hr.salaries --property test --value value
```

#### Remove a tables's property

```bash
gcli table remove --name catalog_postgres.hr.salaries --property test
```

#### Create a table

```bash
gcli table create --name catalog_postgres.hr.salaries --comment "comment" --columnfile ~/table.csv
```

### User commands

#### Create a user

```bash
gcli user create --user new_user
```

#### Show a user's details

```bash
gcli user details --user new_user
```

#### List all users

```bash
gcli user list
```

#### Show a roles's audit information

```bash
gcli user details --user new_user --audit
```

#### Delete a user

```bash
gcli user delete --user new_user
```

### Group commands

#### Create a group

```bash
gcli group create --group new_group
```

#### Display a group's details

```bash
gcli group details --group new_group
```

#### List all groups

```bash
gcli group list
```

#### Show a groups's audit information

```bash
gcli group details --group new_group --audit
```

#### Delete a group

```bash
gcli group delete --group new_group
```

### Tag commands

#### Display a tag's details

```bash
gcli tag details --tag tagA
```

#### Create tags

```bash
gcli tag create --tag tagA tagB
```

#### List all tags

```bash
gcli tag list
```

#### Delete tags

```bash
gcli tag delete --tag tagA tagB
```

#### Add tags to an entity

```bash
gcli tag set --name catalog_postgres.hr --tag tagA tagB
```

#### Remove tags from an entity

```bash
gcli tag remove --name catalog_postgres.hr --tag tagA tagB
```

#### Remove all tags from an entity

```bash
gcli tag remove --name catalog_postgres.hr
```

#### List all tags on an entity

```bash
gcli tag list --name catalog_postgres.hr
```

#### List the properties of a tag

```bash
gcli tag properties --tag tagA
```

#### Set a properties of a tag

```bash
gcli tag set --tag tagA --property test --value value
```

#### Delete a property of a tag

```bash
gcli tag remove --tag tagA --property test
```

#### Rename a tag

```bash
gcli tag update --tag tagA --rename newTag
```

#### Update a tag's comment

```bash
gcli tag update --tag tagA --comment "new comment"
```

### Owner commands

#### List an owner

```bash
gcli catalog details --owner --name postgres
```

#### Set an owner to a user

```bash
gcli catalog set --owner --user admin --name postgres
```

#### Set an owner to a group

```bash
gcli catalog set --owner --group groupA --name postgres
```

### Role commands

When granting or revoking privileges the following privileges can be used.

create_catalog, use_catalog, create_schema, use_schema, create_table, modify_table, select_table, create_fileset, write_fileset, read_fileset, create_topic, produce_topic, consume_topic, manage_users, create_role, manage_grants

Note that some are only valid for certain entities.

#### Display role details

```bash
gcli role details --role admin
```

#### List all roles

```bash
gcli role list
```

#### Show a roles's audit information

```bash
gcli role details --role admin --audit
```

#### Create a role

```bash
gcli role create --role admin
```

#### Delete a role

```bash
gcli role delete --role admin
```

#### Add a role to a user

```bash
gcli user grant --user new_user --role admin
```

#### Remove a role from a user

```bash
gcli user revoke --user new_user --role admin
```

#### Add a role to a group

```bash
gcli group grant --group groupA --role admin
```

#### Remove a role from a group

```bash
gcli group revoke --group groupA --role admin
```

### Grant a privilege

```bash
gcli role grant --name catalog_postgres --role admin --privilege create_table modify_table
```

### Revoke a privilege

```bash
gcli role revoke --metalake metalake_demo --name catalog_postgres --role admin --privilege create_table modify_table
```

### Topic commands

#### Display a topic's details

```bash
gcli topic details --name kafka.default.topic3
```

#### Create a topic

```bash
gcli topic create --name kafka.default.topic3
```

#### List all topics

```bash
gcli topic list --name kafka.default
```

#### Delete a topic

```bash
gcli topic delete --name kafka.default.topic3
```

#### Change a topic's comment

```bash
gcli topic update --name kafka.default.topic3 --comment new_comment
```

#### Display a topics's properties

```bash
gcli topic properties --name kafka.default.topic3
```

#### Set a topics's property

```bash
gcli topic set --name kafka.default.topic3 --property test --value value
```

#### Remove a topics's property

```bash
gcli topic remove --name kafka.default.topic3 --property test
```

### Fileset commands

#### Create a fileset

```bash
gcli fileset create --name hadoop.schema.fileset --properties managed=true,location=file:/tmp/root/schema/example
```

#### List filesets

```bash
gcli fileset list --name hadoop.schema
```

#### Display a fileset's details

```bash
gcli fileset details --name hadoop.schema.fileset
```

#### Delete a fileset

```bash
gcli fileset delete --name hadoop.schema.fileset
```

#### Update a fileset's comment

```bash
gcli fileset update --name hadoop.schema.fileset --comment new_comment
```

#### Rename a fileset

```bash
gcli fileset update --name hadoop.schema.fileset --rename new_name
```

#### Display a fileset's properties

```bash
gcli fileset properties --name hadoop.schema.fileset 
```

#### Set a fileset's property

```bash
gcli fileset set  --name hadoop.schema.fileset --property test --value value
```

#### Remove a fileset's property

```bash
gcli fileset remove --name hadoop.schema.fileset --property test
```

### Column commands

Note that some commands are not supported depending on what the database supports.

When setting the datatype of a column the following basic types are currently supported:
null, boolean, byte, ubyte, short, ushort, integer, uinteger, long, ulong, float, double, date, time, timestamp, tztimestamp, intervalyear, intervalday, uuid, string, binary

In addition decimal(precision,scale), fixed(length), fixedchar(length) and varchar(length).


#### Show all columns

```bash
gcli column list --name catalog_postgres.hr.departments
```

#### Show column's audit information

```bash
gcli column details --name catalog_postgres.hr.departments.name --audit
```

#### Show a column's audit information

```bash
gcli column details --name catalog_postgres.hr.departments.name --audit
```

#### Add a column

```bash
gcli column create --name catalog_postgres.hr.departments.value --datatype long
gcli column create --name catalog_postgres.hr.departments.money --datatype "decimal(10,2)"
gcli column create --name catalog_postgres.hr.departments.name --datatype "varchar(100)"
gcli column create --name catalog_postgres.hr.departments.fullname --datatype "varchar(250)" --default "Fred Smith" --null=false
```

#### Delete a column

```bash
gcli  column delete --name catalog_postgres.hr.departments.money
```

#### Update a column

```bash
gcli column update --name catalog_postgres.hr.departments.value --rename values
gcli column update --name catalog_postgres.hr.departments.values --datatype "varchar(500)"
gcli column update --name catalog_postgres.hr.departments.values --position name
gcli column update --name catalog_postgres.hr.departments.name --null true
```

#### Simple authentication

```bash
gcli <normal command> --simple
```

### Authentication

#### Simple authentication with user name

```bash
gcli <normal command> --simple --login userName
```