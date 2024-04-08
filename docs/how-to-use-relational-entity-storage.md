---
title: How to use relational entity storage
slug: /how-to-use-relational-entity-storage
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

Gravitino supports using `Relational Entity Storage` to store metadata after version `0.5.0`.  

#### Target users
`Relational Entity Storage` is mainly aimed at users who are accustomed to using RDBMS to store data or lack available KV storage
to help them better to use Gravitino.  

#### Advantages
With `Relational Entity Storage`, you can quickly deploy Gravitino in a production environment and take advantage of relational
storage to manage metadata.  
Currently, you can use `MySQL` as the jdbc backend for `Relational Entity Storage`.

## Steps for usage

#### Prerequisites

+ MySQL 5.7 or 8.0
+ Gravitino distribution package
+ MySQL connector Jar (Should be compatible with your version of MySQL)

#### Step 1: Get the initialization script

You need `download` and `unzip` the distribution package firstly, please see: [How to install Gravitino](how-to-install.md).
Then you can get the initialization script in the directory:
```text
${distribution_package_directory}/scripts/relational-entity-store/mysql/
```
The script names like `schema-{version}-mysql.sql`, and the `version` depends on your Gravitino version.  
For example, if your Gravitino version is `0.5.0`, then you should choose the `schema-0.5.0-mysql.sql` script.

#### Step 2: Initialize the database

Please `create a database` in MySQL in advance, and `execute` the initialization script obtained above in the database.

#### Step 3: Place the MySQL connector Jar

You should `download` the MySQL connector Jar for the corresponding version of MySQL you use (You can download it from the [maven-central-repo](https://repo1.maven.org/maven2/mysql/mysql-connector-java/)), which names like `mysql-connector-java-{driver-version}.jar`.  
Then please place it in the distribution package directory:
```text
${distribution_package_directory}/libs/
```

#### Step 4: Set up the Gravitino server configs

Find the server configuration file names `gravitino.conf` in the distribution package directory:

```text
${distribution_package_directory}/conf/
```
Then set up the following server configs:
```text
gravitino.entity.store = relational
gravitino.entity.store.relational = JDBCBackend
gravitino.entity.store.relational.jdbcUrl = ${your_jdbc_url}
gravitino.entity.store.relational.jdbcDriver = ${your_driver_name}
gravitino.entity.store.relational.jdbcUser = ${your_username}
gravitino.entity.store.relational.jdbcPassword = ${your_password}
```

#### Step 4: Start the server

Finally, you can run the script in the distribution package directory to start the server:

```shell
./${distribution_package_directory}/bin/gravitino.sh start
```

