---
title: "Gravitino connector development"
slug: /trino-connector/development
keyword: gravitino connector development 
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

This document is to guide users through the development of the Gravitino connector for Trino locally.

## Prerequisites

Before you start developing the Gravitino trino connector, you need to have the following prerequisites:

1. You need to start the Gravitino server locally, for more information, please refer to the [start Gravitino server](../how-to-install.md)
2. Create a catalog in the Gravitino server, for more information, please refer to the [Gravitino metadata management](../manage-metadata-using-gravitino.md). Assuming we have just created a MySQL catalog using the following command:

```curl
curl -X POST -H "Content-Type: application/json" -d '{"name":"test","comment":"comment","properties":{}}' http://localhost:8090/api/metalakes

curl -X POST -H "Content-Type: application/json" -d '{"name":"mysql_catalog3","type":"RELATIONAL","comment":"comment","provider":"jdbc-mysql", "properties":{
  "jdbc-url": "jdbc:mysql://127.0.0.1:3306?useSSL=false&allowPublicKeyRetrieval=true",
  "jdbc-user": "root",
  "jdbc-password": "123456",
  "jdbc-driver": "com.mysql.cj.jdbc.Driver"
}}' http://localhost:8090/api/metalakes/test/catalogs
```

:::note
Please change the above `localhost`, `port` and the names of metalake and catalogs accordingly.
:::


## Development environment

To develop the Gravitino connector locally, you need to do the following steps:

### IDEA

1. Clone the Trino repository from the [GitHub](https://github.com/trinodb/trino) repository. We advise you to use the release version 426 or 435. 
2. Open the Trino project in your IDEA.
3. Create a new module for the Gravitino connector in the Trino project as the following picture (we will use the name `trino-gravitino` as the module name in the following steps). ![trino-gravitino](../assets/trino/create-gravitino-connector.jpg)
4. Add a soft link to the Gravitino trino connector module in the Trino project. Assuming the src java main directory of the Gravitino trino connector in project Gravitino is `gravitino/path/to/gravitino-trino-connector/src/main/java`, 
and the src java main directory of trino-gravitino in the Trino project is `trino/path/to/trino-gravitino/src/main/java`, you can use the following command to create a soft link:

```shell
ln -s gravitino/path/to/trino-connector/src/main/java trino/path/to/trino-gravitino/src/main/java
```
then you can see the `gravitino-trino-connecor` source files and directories in the `trino-gravitino` module as follows:

![trino-gravitino-structure](../assets/trino/add-link.jpg)

5. Change the `pom.xml` file in the `trino-gravitino` module accordingly. This is a [example](../assets/trino/pom.xml) of the `pom.xml` file in the `trino-gravitino` module.
6. Try to compile module `trino-gravitino` to see if there are any errors. 
```shell
# build the whole trino project
./mvnw -pl '!core/trino-server-rpm' package -DskipTests -Dair.check.skip-all=true


# build the trino-gravitino module if we change the code in the trino-gravitino module
./mvnw clean -pl 'plugin/trino-gravitino' package -DskipTests -Dcheckstyle.skip -Dair.check.skip-checkstyle=true -DskipTests -Dair.check.skip-all=true
```
7. Set up the configuration for the Gravitino connector in the Trino project. You can do as the following picture shows:
![](../assets/trino/add-config.jpg)

The corresponding configuration files are here: [gravitino.properties](../assets/trino/gravitino.properties) and [config.properties](../assets/trino/config.properties).

8. Start the Trino server and connect to the Gravitino server.
![](../assets/trino/start-trino.jpg)
9. If `DevelopmentServer` has started successfully, you can connect to the Trino server using the `trino-cli` and run the following command to see if the Gravitino connector is available:
```shell
java -jar trino-cli-429-executable.jar --server localhost:8180
```
:::note
The `trino-cli-429-executable.jar` is the Trino CLI jar file, you can download it from the [Trino release page](https://trino.io/docs/current/client/cli.html). **Users can use the version of the Trino CLI jar file according to the version of the Trino server.**
:::

10. If nothing goes wrong, you can start developing the Gravitino connector in the Gravitino project and debug it in the Trino project.
![](../assets/trino/show-catalogs.jpg)
