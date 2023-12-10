---
title: "Gravitino connector installation"
slug: /trino-connector/install
keyword: gravition connector trino
license: Copyright 2023 Datastrato Pvt. This software is licensed under the Apache License version 2.
---
# Installation

To install the Gravitino connector, first deploy the Trino environment, and then install the Gravitino connector plugin into Trino.
Please refer to the [Deploying Trino documentation](https://trino.io/docs/current/installation/deployment.html).

1. Download the Gravitino connector tarball and unpack it.
   The tarball contains a single top-level directory `gravitino-trino-connector-xxx`,
   which we call the connector directory.
   [Download the gravitino-connector](https://github.com/datastrato/gravitino/releases).
2. Copy the connector directory to the Trino's plugin directory.
   Normally, the directory location is `Trino-server-xxx/plugin`, and the directory contains other catalogs used by Trino.
3. Add Trino JVM arguments `-Dlog4j.configurationFile=file:///xxx/log4j2.properties` to enable logging for the Gravitino connector.

Alternatively,
you can build the Gravitino connector package from sources
and obtain the `gravitino-trino-connector-xxx.tar.gz` file in the `$PROJECT/distribution` directory.
Please refer to the [Gravitino Development documentation](how-to-build)

## Example
Let me tell you how to install the Gravitino connector in Trino office docker images.

### Running the container
Use the docker command to create a container from the `trinodb/trino` image. Assign it the trino-gravitino name. 
Run it in the background, and map the default Trino port, which is 8080, from inside the container to port 8080 on your workstation.

```shell
docker run --name trino-gravitino -d -p 8080:8080 trinodb/trino:426
```
Run docker ps to check whether the container is running.


### Installing the Gravitino connector
Download the Gravitino connector tarball and unpack it.

```shell
cd /tmp
wget xxx/gravitino-trino-connector-0.3.0.tar.gz
tar -zxvf gravitino-trino-connector-0.3.0.tar.gz
```
You can see the connector directory `gravitino-trino-connector-0.3.0` after unpacking.

Copy the connector directory to the Trino container's plugin directory.
```shell
docker copy  /tmp/gravitino-trino-connector-0.3.0 trino-gravitino:/lib/trino/plugin
```

Check the plugin directory in the container.
```shell
docker exec -it trino-gravitino /bin/bash
cd /lib/trino/plugin
```
Now you can see the Gravitino connector directory in the plugin directory.

### Configuring the Gravitino connector
Assuming you have now started the Gravitino server on the host `gravition-server-host` and can access it properly. If not, please refer to the [Gravitino documentation](xx)

Add catalog configuration to the Trino configuration file `/etc/trino/catalog/gravitino.properties`.
```text
connector.name=gravitino
gravitino.url=http://gravition-server-host:8090
gravitino.metalake=test
```
The `gravitino.name` defines the connector of Gravitino connector are used.
The `gravitino.metalake` defines which metalake are used. It should exist in the Gravitino server.
The `gravitino.uri` defines the connection information about Gravitino server. Make use your container can access the Gravitino server.

If you don't have the `test` metalake. You can create a new metalake named `test`. 
You should start the gravitino server, see more "docs/gravitino/overview.md#running-the-server". 

Create a new metalake named `test` by the following command.
```shell
curl -X POST -H "Content-Type: application/json" -d '{"name":"test","comment":"comment","properties":{}}' http://gravition-server-host:8090/api/metalakes
```

Restart the Trino container to load the Gravitino connector.

```shell
docker restart trino-gravitino
```

### Verifying the Gravitino connector
Assummimg you have created a catalog named `test.jdbc-mysql` in the Gravitino server.
Use the Trino CLI to connect to the Trino container and run a query.
```text
docker exec -it trino trino
trino> show catalogs;
Catalog
------------------------
gravitino
jmx
memory
tpcds
tpch
system
```
You can see the `gravitino` catalog in the result. It means the Gravitino connector is installed successfully.

Assuming you have created a catalog named `test.jdbc-mysql` in the Gravitino server. 

Then you can use the Trino CLI to connect to the Trino container and run a query.
```text
docker exec -it trino trino
trino> show catalogs;
Catalog
------------------------
gravitino
jmx
memory
tpcds
tpch
system
test.jdbc-mysql
```
The catalog named 'test.jdbc-mysql' is your created catalog by gravitino server. 
You can use it to access the mysql database like other Trino catalogs.


