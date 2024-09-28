---
title: "Apache Gravitino Trino connector installation"
slug: /trino-connector/installation
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

To install the Apache Gravitino Trino connector, you should first deploy the Trino environment, and then install the Gravitino Trino connector plugin into Trino.
Please refer to the [Deploying Trino documentation](https://trino.io/docs/current/installation/deployment.html) and do the following steps:

1. [Download](https://github.com/apache/gravitino/releases) the Gravitino Trino connector tarball and unpack it.
   The tarball contains a single top-level directory `gravitino-trino-connector-<version>`,
   which called the connector directory.
2. Copy the connector directory to the Trino's plugin directory.
   Normally, the directory location is `Trino-server-<version>/plugin`, and the directory contains other catalogs used by Trino.
3. Add Trino JVM arguments `-Dlog4j.configurationFile=file:////etc/trino/log4j2.properties` to enable logging for the Gravitino Trino connector.
4. Update Trino coordinator configuration. 
   You need to set `catalog.management=dynamic`, The config location is `Trino-server-<version>/etc/config.properties`,  and the contents like:

```text
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
catalog.management=dynamic
discovery.uri=http://0.0.0.0:8080
```

Alternatively,
you can build the Gravitino Trino connector package from the sources
and obtain the `gravitino-trino-connector-<version>.tar.gz` file in the `$PROJECT/distribution` directory.
Please refer to the [Gravitino Development documentation](../how-to-build.md)

## Example

You can install the Gravitino Trino connector in Trino office docker images step by step.

### Running the container

Use the docker command to create a container from the `trinodb/trino` image. Assign it the trino-gravitino name.
Run it in the background, and map the default Trino port, which is 8080, from inside the container to port 8080 on your machine.

```shell
docker run --name trino-gravitino -d -p 8080:8080 trinodb/trino:435
```

Run `docker ps` to check whether the container is running.


### Installing the Apache Gravitino Trino connector

Download the Gravitino Trino connector tarball and unpack it.

```shell
cd /tmp
wget https://github.com/apache/gravitino/releases/gravitino-trino-connector-<version>.tar.gz
tar -zxvf gravitino-trino-connector-<version>.tar.gz
```

You can see the connector directory `gravitino-trino-connector-<version>` after unpacking.

Copy the connector directory to the Trino container's plugin directory.

```shell
docker cp /tmp/gravitino-trino-connector-<version> trino-gravitino:/lib/trino/plugin
```

Check the plugin directory in the container.

```shell
docker exec -it trino-gravitino /bin/bash
cd /lib/trino/plugin
```

Now you can see the Gravitino Trino connector directory in the plugin directory.

### Configuring the Trino

You can find the Trino configuration file `config.properties` in the directory `/etc/trino`. You need changed the file like this:

```text
#single node install config
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
catalog.management=dynamic
```

### Configuring the Apache Gravitino Trino connector

Assuming you have now started the Gravitino server on the host `gravitino-server-host` and already created a metalake named `test`, if those have not been prepared, please refer to the [Gravitino getting started](../getting-started.md).

To configure Gravitino Trino connector correctly, you need to put the following configurations to the Trino configuration file `/etc/trino/catalog/gravitino.properties`.

```text
connector.name=gravitino
gravitino.uri=http://gravitino-server-host:8090
gravitino.metalake=test
```

- The `gravitino.name` defines which Gravitino Trino connector is used. It must be `gravitino`.
- The `gravitino.metalake` defines which metalake are used. It should exist in the Gravitino server.
- The `gravitino.uri` defines the connection information about Gravitino server. Make sure your container can access the Gravitino server.

Full configurations for Apache Gravitino Trino connector can be seen [here](configuration.md)

If you haven't created the metalake named `test`, you can use the following command to create it.

```shell
curl -X POST -H "Content-Type: application/json" -d '{"name":"test","comment":"comment","properties":{}}' http://gravitino-server-host:8090/api/metalakes
```

And then restart the Trino container to load the Gravitino Trino connector.

```shell
docker restart trino-gravitino
```

### Verifying the Apache Gravitino Trino connector

Use the Trino CLI to connect to the Trino container and run a query.

```text
docker exec -it trino-gravitino trino
trino> SHOW CATALOGS;
Catalog
------------------------
gravitino
jmx
memory
tpcds
tpch
system
```

You can see the `gravitino` catalog in the result set. This signifies the successful installation of the Gravitino Trino connector.

Assuming you have created a catalog named `test.jdbc-mysql` in the Gravitino server, or please refer to [Create a Catalog](../manage-relational-metadata-using-gravitino.md#create-a-catalog). Then you can use the Trino CLI to connect to the Trino container and run a query like this.

```text
docker exec -it trino-gravitino trino
trino> SHOW CATALOGS;
Catalog
------------------------
gravitino
jmx
memory
tpcds
tpch
system
jdbc-mysql
```

The catalog named 'jdbc-mysql' is the catalog that you created by gravitino server, and you can use it to access the mysql database like other Trino catalogs.
