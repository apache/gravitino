---
title: 'Command Line Interface (CLI)'
slug: /cli
keyword: cli
last_update:
  date: 2024-10-23
  author: justinmclean
license: 'This software is licensed under the Apache License version 2.'
---

This document shows you how to manage metadata in Apache Gravitino using its Command Line Interface (CLI).
The CLI offers a terminal based alternative to using code or the REST interface for metadata management.

## Usage

The general syntax for CLI commands is `gcli <entity> <command> [options]`.

 ```bash
  usage: gcli [metalake|catalog|schema|model|table|column|user|group|tag|topic|fileset] [list|details|create|delete|update|set|remove|properties|revoke|grant] [options]
  Options
 usage: gcli
  -a,--audit             display audit information
    --alias <arg>        model aliases
    --all                all operation for --enable
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
 -r,--role <arg>         role name
    --rename <arg>       new entity name
 -s,--server             Gravitino server version
    --simple             simple authentication
    --sortorder          display sortorder information
 -t,--tag <arg>          tag name
 -u,--url <arg>          Gravitino URL (default: http://localhost:8090)
    --uri <arg>          model version artifact
 -v,--version            Gravitino client version
 -V,--value <arg>        property value
 -x,--index              display index information
 -z,--provider <arg>     provider one of hadoop, hive, mysql, postgres,
                         iceberg, kafka
 ```

:::tip
You can configure an alias for the CLI for ease of use.
For example:

```bash
alias gcli='java -jar ../../cli/build/libs/gravitino-cli-*-incubating-SNAPSHOT.jar'
```

Or you can use the `gcli.sh` script which can be found in the `clients/cli/bin/` directory.
:::

## Command Overview

The following commands are used for entity management:

- list: List available entities
- create: Create a new entity
- details: Show detailed information about an entity
- update: Update an existing entity
- delete: Delete an existing entity
- properties: Display an entities properties
- set: Set a property on an entity
- remove: Remove a property from an entity

For commands on a specific type of entity, please check the details:

- [generic commands](./cli-reference/generic.md)
- [auth commands](./cli-reference/auth.md)

- [metalake commands](./cli-reference/metalake.md)
- [catalog commands](./cli-reference/catalog.md)
- [schema commands](./cli-reference/schema.md)
- [table commands](./cli-reference/table.md)
- [topic commands](./cli-reference/topic.md)
- [filesetcommands](./cli-reference/fileset.md)
- [column commands](./cli-reference/column.md)
- [tag commands](./cli-reference/tag.md)

- [user commands](./cli-reference/user.md)
- [group commands](./cli-reference/group.md)
- [role commands](./cli-reference/role.md)
- [owner commands](./cli-reference/owner.md)

## Frequently used options

### The Gravitino URL

The Gravitino server URL has to be provided to every CLI command.
You can specify the server URL in several ways.
The CLI checks the URL setting in the following order:

1. Use the `--url` argument on the command line, if specified.
1. Extract the URL from the `GRAVITINO_URL` environment variable, if set.
1. Check the `URL` option in the [Gravitino CLI configuration file](#cli-configuration-file).

### The authentication type

The Gravitino server supports several [authentication-types](../security/authentication.md).
You can specify the authentication type in several ways.
The CLI checks the authentication type in the following order:

1. Check the `--simple` argument on the command line, if specified.
2. Read the `GRAVITINO_AUTH` environment variable, if set.
3. Check the `auth` option in the [Gravitino CLI configuration file](#cli-configuration-file).

You can configure OAuth authentication in the configuration file,
as shown in the following example:

```text
# Authentication
auth=oauth
serverURI=http://127.0.0.1:1082
credential=xx:xx
token=test
scope=token/test
```

You can also set up Kerberos authentication in the configuration file,
as shown in the following example:

```text
# Authentication
auth=kerberos
principal=user/admin@foo.com
keytabFile=file.keytab
```

### Client/server version mismatch

If the client and server are running different versions of the Gravitino software,
you may need to ignore the client/server version check for the command to run.
This can be done in several ways:

1. Use the `--ignore` argument on the command line.
1. Set the `GRAVITINO_IGNORE` environment variable.
1. Set the `ignore=true` option in the [Gravitino CLI configuration file](#cli-configuration-file).

### The Metalake name

In a typical scenario, a user work with a single Metalake.
If this is the case, you can set the Metalake name in several ways
so you don't need to repeat the same parameter in your commands.
The CLI checks the metalake specified in the following order:

1. Use the `--metalake` argument on the command line, if specified.
1. Parse the `GRAVITINO_METALAKE` environment variable, if set.
1. Check the `metalake` setting in the [Gravitino CLI configuration file](#cli-configuration-file).

## CLI configuration file

The gravitino CLI can read frequently used CLI options from a configuration file.
By convention, the file is `.gravitino` in the user's HOME directory.
The metalake, URL and ignore parameters can be set in this file.

```text
# Gravitino CLI configuration file

# Gravitino server to connect to
URL=http://localhost:8090

# Authentication
auth=simple

# Metalake to use
metalake=metalake_demo

# Ignore client/server version mismatch
ignore=true
```

### Potentially unsafe operations

For operations that delete data or rename a metalake,
the user will be prompted to confirm that they wish to run this command.
The `--force` option can be specified to override this behaviour.

All the commands are performed by using the [Java API](../api/java-api) internally.

### Multiple properties

For commands that accept multiple properties they can be specified in a couple of different ways:

1. <tt>gcli --properties n1=v1,n2=v2,n3=v3</tt>
2. <tt>gcli --properties n1=v1 n2=v2 n3=v3</tt>
3. <tt>gcli --properties n1=v1 --properties n2=v2 --properties n3=v3</tt>

### Setting properties and tags

Different options are needed to add a tag and set a property of a tag with `gcli tag set`.
To add a tag, specify the tag (via `--tag`) and the entity to tag (via `--name`).
To set the property of a tag (via `--tag`), you need to specify the property (via `--property`)
and value (via `--value`) you want to set.

To delete a tag, again, you need to specify the tag and entity.
To remove a tag's property, you need to select the tag and property.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=CLI" alt="" />

