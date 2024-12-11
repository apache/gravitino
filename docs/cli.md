---
title: 'Apache Gravitino Command Line Interface'
slug: /cli
keyword: cli
last_update:
  date: 2024-10-23
  author: justinmclean
license: 'This software is licensed under the Apache License version 2.'
---

This document provides guidance on managing metadata within Apache Gravitino using
the Command Line Interface (CLI). The CLI offers a terminal based alternative to
using code or the REST API for metadata management.

Currently, the CLI allows users to view metadata information for metalakes, catalogs,
schemas, tables, users, roles, groups, tags, topics and filesets.
Future updates will expand on these capabilities.

## Running the CLI

You can configure an alias for the CLI for ease of use.

```bash
alias gcli='java -jar ../../cli/build/libs/gravitino-cli-*-incubating-SNAPSHOT.jar'
```

Or you use the `gcli.sh` script located in the `clients/cli/bin/` directory.

## Usage

The general syntax for the Gravitino CLI is `gcli entity command [options]`.

```none
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

## Commands Actions

The following commands are used for entity management:

- `list`: List available entities
- `details`: Show detailed information about an entity
- `create`: Create a new entity
- `delete`: Delete an existing entity
- `update`: Update an existing entity
- `set`: Set a property on an entity
- `remove`: Remove a property from an entity
- `properties`: Display an entities properties

### Setting the Metalake name

Considering that managing a metalake is a typical scenario, you can set the metalake
name in several ways so it doesn't need to be passed on the command line.

1. Specify the `--metalake` argument on the command line.
1. Set the `GRAVITINO_METALAKE` environment variable.
1. Place it in the Gravitino [CLI configuration file](#config-file).

The command line option overrides the environment variable and the environment variable
takes precedence over the configuration file.

### Setting the Gravitino URL

To avoid specifying the Gravitino URL using `--url` argument for every command,
you can persist the URL setting.

1. Set the `GRAVITINO_URL` environment variable.
1. Place it in the Gravitino [CLI configuration file](#config-file).

The command line option overrides the environment variable and the environment variable
overrides the configuration file.

### Setting the Gravitino Authentication Type

The authentication type can also be set in several ways.

1. Specify the `--simple` argument in the command line.
1. Set the `GRAVITINO_AUTH` environment variable.
1. Place it in the Gravitino [CLI configuration file](#config-file).

<!--TODO: link to docs on auth type-->

### Gravitino CLI configuration file {#config-file}

The gravitino CLI can read commonly used CLI options from a configuration file.
By default, the file is `.gravitino` in the user's home directory.

```ini
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

The OAuth authentication can also be configured in the configuration file,
as shown in the following example:

```ini
# Authentication
auth=oauth
serverURI=http://127.0.0.1:1082
credential=xx:xx
token=test
scope=token/test
```

The Kerberos authentication can be configured in the configuration file,
as shown in the following example:

```ini
# Authentication
auth=kerberos
principal=user/admin@foo.com
keytabFile=file.keytab
```

### Potentially unsafe operations

For operations that delete data or rename a metalake, the users will be prompted to
make sure they wish to run this command.
The `--force` option can be specified to override this behaviour.

### Manage metadata

All the commands are performed by using the [Java API](api/java-api) internally.

### Show help

To view the help on command usage:

```bash
gcli --help
```

### Show version

`gcli --version`
: Show the client version.

`gcli --server`
: Show the server version.

:::note
**Client/server version mismatch**

If the client and server are running different versions of the Gravitino software,
you may need to ignore the client/server version check for the command to be run.
This can be done in several ways:

1. Specify the `--ignore` argument in the command line.
1. Set the `GRAVITINO_IGNORE` environment variable.
1. Place it in the Gravitino CLI configuration file.
:::

### Multiple properties

For commands that accept multiple properties they can be specified in a couple of different ways:

1. `gcli --properties n1=v1,n2=v2,n3=v3`
1. `gcli --properties n1=v1 n2=v2 n3=v3`
1. `gcli --properties n1=v1 --properties n2=v2 --properties n3=v3`

### Setting properties and tags

Different options are needed to add a tag or to set a property for a tag using `gcli tag set`.
To add a tag, specify the tag (via `--tag`) and the entity to tag (via `--name`).
To set the property of a tag (via `--tag`) you need to specify the property (via `--property`)
and value (via `--value`) you want to set.

To delete a tag, again, you need to specify the tag and the entity; to remove a tag's property,
you need to select the tag and property.

## Command examples

Please set the metalake in the Gravitino configuration file or the environment variable before
running any of these commands.

### Metalake commands

`gcli metalake create --metalake <name> --comment <comment>`
: Create a metalake.

`gcli metalake properties`
: Show the properties for a metalake.

`gcli metalake delete`
: Delete a metalake.

`gcli metalake list`
: List all metalakes

`gcli metalake details`
: Check the details of a specific a metalake.

`gcli metalake details --audit`
: Show the audit information for a metalake

`gcli metalake update --rename <new-name>`
: Rename a metalake

`gcli metalake update --comment "<new comment>"`
: Update the comment for a metalake

`gcli metalake set --property <property> --value <value>`
: Set the property for a metalake.

`gcli metalake remove --property <name>`
: Remove the specified property from a metalake.

### Catalog commands

`gcli catalog list`
: Show all catalogs in a metalake

`gcli catalog details --name <name>`
: Show a catalog details

`gcli catalog details --name <name> --audit`
: Show a catalog audit information

#### Creating a catalog

The type of catalog to be created is specified by the `--provider` option.

`gcli catalog create --provider hive --name <name> --properties <property key-value pairs>`
: Create a catalog.

Different catalogs require different properties.
For example, a Hive catalog requires a `--metastore-uris` property.
 
- For a Hive catalog, you need to specify the `metastore.uris` property. For example:

  `metastore.uris=thrift://hive-host:9083`

- For an Icebeg catalog, you need to specify the `uri`property. For example:

  `uri=thrift://hive-host:9083,catalog-backend=hive,warehouse=hdfs://hdfs-host:9000/user/iceberg/warehouse`

- For a MySQL Postgres catalog, you need to specify the `jdbc-url`, `jdbc-user`, `jdbc-password`,
  `jdbc-driver` property. For example:

  `jdbc-url=jdbc:mysql://mysql-host:3306?useSSL=false,jdbc-user=user,jdbc-password=password,jdbc-driver=com.mysql.cj.jdbc.Driver`
 
- For a Postgres catalog, you need to specify the `jdbc-url`, `jdbc-user`, `jdbc-password`,
  `jdbc-driver`, `db` property. For example:

  `jdbc-url=jdbc:postgresql://postgresql-host/mydb,jdbc-user=user,jdbc-password=password,jdbc-database=db,jdbc-driver=org.postgresql.Driver`

- For a Kafka catalog, you need to specify the `boostrap.servers` property. For example:

  `bootstrap.servers=127.0.0.1:9092,127.0.0.2:9092`

- For a Doris catalog, you need to specify the `jdbc-driver`, `jdbc-url`, `jdbc-user`, `jdb-password` property.
  For example:

  `jdbc-url=jdbc:mysql://localhost:9030,jdbc-driver=com.mysql.jdbc.Driver,jdbc-user=admin,jdbc-password=password`

- For a Paimon catalog, you need to specify the `catalog-backend`, `uri`, `authentication.type` properties.
  For example:

  `catalog-backend=jdbc,uri=jdbc:mysql://127.0.0.1:3306/metastore_db,authentication.type=simple`

- For a Hudi catalog, you need to specify the `catalog-backend`, `uri` properties. For example:

   `catalog-backend=hms,uri=thrift://127.0.0.1:9083`

- For an Oceanbase catalog, you need to specify the `jdbc-driver`, `jdbc-url`, `jdbc-user`, and
  `jdbc-password` properties. For example:

  `jdbc-url=jdbc:mysql://localhost:2881,jdbc-driver=com.mysql.jdbc.Driver,jdbc-user=admin,jdbc-password=password`

`gcli catalog delete --name <name>`
: Delete a catalog.

`gcli catalog update --name <old-name> --rename <new-name>`
:Rename a catalog.

`gcli catalog update --name <name> --comment "<new comment>"`
: Change the comment for a catalog.

`gcli catalog properties --name <name>`
: List the properties for a catalog.

`gcli catalog set --name <name> --property <property> --value <value>`
: Set a property for a catalog.

`gcli catalog remove --name <name> --property <property>`
: Remove a property from a catalog.

### Schema commands

`gcli schema create --name <schema>`
: Create a schema.

`gcli schema properties --name <schema> -i`
: Display the properties about a schema.

`gcli schema list --name <catalog>`
: List all schemas in the specified catalog.

`gcli schema details --name <schema>`
: Show the details about schemas

`gcli schema details --name <schema> --audit`
: Show schema audit information

:::note
Setting and removing schema properties is currently not supported in the Java API
or the Gravitino CLI.
:::

### Table commands

When creating a table, the columns are specified in a CSV file containing the properties
of each column, including

- the column name (required)
- the data type (required)
- the comment string that defaults to null
- a string format boolean indicating whether the column can be null (default=true)
- a string format boolean indicating whether the column is auto-incremented (default=false)
- the default value
- the default data type, defaults to the column data type if a default value is specified.
 
Example CSV file:

```text
Name,Datatype,Comment,Nullable,AutoIncrement,DefaultValue,DefaultType
name,String,person's name
ID,Integer,unique id,false,true
location,String,city they work in,false,false,Sydney,String
```

In the commands below, note that the table name is in the format
`<catalog>.<schema>.<table>`.

`gcli table create --name <table> --comment "<comment>" --columnfile <csv-file>`
: Create a table. Note that the table name is in the format `<catalog>.<schema>.<table>`.

`gcli table list --name <schema>`
: List all tables for a schema.

`gcli table details --name <table>`
: Show the details about a table. 

`gcli table details --name <table> --audit`
: Show the audit information for a specified table.

`gcli table details --name <table> --distribution`
: Show distribution information for a specified table.

`gcli table details --name <table> --partition`
: Show the partition information for a specified table.

`gcli table details --name <table> --sortorder`
: Show the sorting order for the table specified.

`gcli table details --name <table> --index`
: Show the index information for the table specified.

`gcli table delete --name <table>`
: Delete a table.

`gcli table properties --name <table>`
: Show the properties for a specific table.

`gcli table set --name <table> --property <property> --value <value>`
: Set a property for a specific table.

`gcli table remove --name <table> --property <property>`
: Remove the specified property from the specified table.

### User commands

`gcli user create --user <user>`
: Create a new user.

`gcli user details --user <user>`
: Show the details about the specified user.

`gcli user details --user <user> --audit`
: Show a roles's audit information

`gcli user list`
: List all users

`gcli user delete --user <user>`
: Delete the specified user.

### Group commands

`gcli group create --group <group>`
: Create a group.

`gcli group details --group <group>`
: Display the details about a group.

`gcli group details --group new_group --audit`
: Show a groups's audit information.

`gcli group list`
: List all groups.

`gcli group delete --group <group>`
: Delete a group.

### Tag commands

`gcli tag create --tag <tag-A> <tag-B>`
: Create tags.

`gcli tag details --tag <tag>`
: Display the details about a tag.

`gcli tag list`
: List all tags.

`gcli tag delete --tag <tag-A> <tag-B>`
: Delete tags.

`gcli tag set --name <entity> --tag tagA tagB`
: Add tags to an entity. The type of the entity is implied by its name.

`gcli tag remove --name <entity> --tag <tagA> <tagB>`
: Remove tags from an entity. The type of the entity is implied by its name.

`gcli tag remove --name <entity>`
: Remove all tags from an entity. The type of the entity is implied by its name.

`gcli tag list --name <entity>`
: List all tags on an entity

`gcli tag properties --tag <tag>`
: List the properties of a tag.

`gcli tag set --tag <tag> --property <property> --value <value>`
: Set a property for a tag.

`gcli tag remove --tag <tag> --property <property>`
: Delete a property from a tag.

`gcli tag update --tag <tag> --rename <new-name>`
: Rename a tag.

`gcli tag update --tag <tag> --comment "<new-comment>"`
: Update the comment for a tag.

### Owner commands

`gcli catalog details --name <catalog> --owner`
: Show the details about the owner for a catalog.

`gcli catalog set --name <catalog> --owner --user <user>`
: Set the specified user as the owner of the specified catalog.

`gcli catalog set --name <catalog> --owner --group <group>`
: Set the specified group as the owner of the catalog.

### Role commands

When granting or revoking privileges the following privileges can be used.

- `create_catalog`
- `use_catalog`
- `create_schema`
- `use_schema`
- `create_table`
- `modify_table`
- `select_table`
- `create_fileset`
- `write_fileset`
- `read_fileset`
- `create_topic`
- `produce_topic`
- `consume_topic`
- `manage_users`
- `create_role`
- `manage_grants`

Note that some are only valid for certain entities.

`gcli role create --role <role>`
: Create a role.

`gcli role details --role <role>`
: Display the details about a role.

`gcli role details --role <role> --audit`
: Show a roles's audit information

`gcli role list`
: List all roles.

`gcli role delete --role <role>` 
: Delete a role.

`gcli user grant --user <user> --role <role>`
: Grant a specified role to the specified user.

`gcli user revoke --user <user> --role <role>`
: Revoke a role from a user.

`gcli group grant --group <group> --role <role>`
: Grant a role to a group.

`gcli group revoke --group <group> --role <role>`
: Revoke a role from a group.

`gcli role grant --name <entity> --role <role> --privilege <privilege1> <privilege2> ...`
: Grant some privileges to a role.

`gcli role revoke --metalake <metalake> --name <name> --role <role> --privilege <privilege1> <privilege2> ...`
: Revoke some privileges from a role.

### Topic commands

The name of a topic is in the format `<catalog>.<schema>.<topic>`.
For Kafka, the `<schema>` is always `default`.

`gcli topic create --name <topic>`
: Create a topic.

`gcli topic details --name <topic>`
: Display the details for a topic.

`gcli topic list --name <schema>`
: List all topics under the specified schema.

`gcli topic delete --name <topic>`
: Delete the specified topic.

`gcli topic update --name <topic> --comment "<new-comment>"`
: Change the comment for a topic.

`gcli topic properties --name <topic>`
: List the properties for a topic

`gcli topic set --name <topic>--property <property> --value <value>`
: Add a property to the specified topic.

`gcli topic remove --name <topic> --property <property>`
: Remove the specified property from the specified topic.

### Fileset commands

The name of a fileset is in the format of `<catalog>.<schema>.<fileset>`.

`gcli fileset create --name <name> --properties <key-value pairs>`
: Create a fileset.

`gcli fileset list --name hadoop.schema`
: List filesets under the specified schema.

`gcli fileset details --name <fileset>`
: Display the details for a fileset.

`gcli fileset delete --name <fileset>`
: Delete the specified fileset.

`gcli fileset update --name <fileset> --comment "<new-comment>"`
: Change the comment for a fileset

`gcli fileset update --name <fileset> --rename <new-name>`
: Rename an existing fileset.

`gcli fileset properties --name <fileset>`
: List the properties for a fileset.

`gcli fileset set  --name <fileset> --property <property> --value <value>`
: Add or update a property on a fileset.

`gcli fileset remove --name <fileset> --property <property>`
: Remove a property from a fileset.

### Column commands

Note that some commands are not supported on all databases.

When setting the data type of a column, the following basic types are currently supported:
`null`, `boolean`, `byte`, `ubyte`, `short`, `ushort`, `integer`, `uinteger`, `long`, `ulong`,
`float`, `double`, `date`, `time`, `timestamp`, `tztimestamp`, `intervalyear`, `intervalday`,
`uuid`, `string`, `binary`.

In addition `decimal(precision,scale)`, `fixed(length)`, `fixedchar(length)`
and `varchar(length)` are supported.

The name of a column is of the format `<catalog>.<schema>.<table>.<column>`.

`gcli column create --name <column> --datatype long`
: Create a column. You can specify the default value using the `--default` property,
  and you can specify whether the column can be null using the `--null` option.
  For example:
  <br/><br/>
  `gcli column create --name catalog_postgres.hr.departments.fullname --datatype "varchar(250)" --default "Fred Smith" --null=false`

`gcli column delete --name <column>`
: Delete a column.

`gcli column list --name <table>`
: List all columns for a table.

`gcli column details --name <table> --audit`
: Show column's audit information

`gcli column update --name <column> --rename <new-name>`
: Rename an existing column.

`gcli column update --name <column> --datatype <datatype>`
: Change the data type for the specified column.

`gcli column update --name <column> --position <position>`
: Change the position for the specified column.

`gcli column update --name <column>--null true`
: Change the nullability of the specified column.

### Authentication Support

`gcli <command> --simple`
: Authenticate using the simple authentication type.

`gcli <command> --simple --login <user>`
: Simple authentication with the specified user name.

