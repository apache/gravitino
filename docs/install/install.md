---
title: Install Apache Gravitino
slug: /install-guide
license: "This software is licensed under the Apache License version 2."
---

## Prerequisites 

- The Apache Gravitino project supports running on Java 8, 11, and 17.
  Make sure you have Java installed and `JAVA_HOME` configured correctly.
  To check the Java version, run the `${JAVA_HOME}/bin/java -version` command.

You can manage these servers independently or run them side by side on the same server.

## Get the Apache Gravitino package

To install Gravitino, you need the Gravitino binary distribution package.
The Gravitino binary distribution package comprises both the Gravitino server and the Gravitino Iceberg REST server.

You can download the latest package from [GitHub](https://github.com/apache/gravitino/releases).
The *Assets* section for a specific tag (e.g. `0.8.0-incubating`) contains a list of package links.

- `gravitino-<version>-bin.tar.gz`:
  The binary package file for Gravitino server, including the Gravitino Iceberg REST server.

- `gravitino-<version>-bin.tar.gz.asc`:
  The PGP signature file for the above binary package.

- `gravitino-<version>-bin.tar.gz.sha512`:
  The SHA512 checksum file for the binary package.

- `gravitino-<version>-src.tar.gz`:
  The source package for the Gravitino project.

- `gravitino-<version>-src.tar.gz.asc`:
  The PGP signature for the source package.

- `gravitino-<version>-src.tar.gz.sha512`:
  The SHA512 checksum file for the source package.

- `gravitino-iceberg-rest-server-<version>-bin.tar.gz`:
  The binary package for the Gravitino Iceberg REST server.

- `gravitino-iceberg-rest-server-<version>-bin.tar.gz.asc`:
  The PGP signature file for the previous binary package.

- `gravitino-iceberg-rest-server-<version>-bin.tar.gz.sha512`:
  The SHA512 checksum file for the previous binary package.

- `gravitino-trino-connector-<version>.tar.gz`:
  The package file for the Gravitino Trino connector.

- `gravitino-trino-connector-<version>.tar.gz.asc`:
  The PGP signature file for the Gravitino Trino connector.

- `gravitino-trino-connector-<version>.tar.gz.sha512`:
  The SHA512 checksum file for the Gravitino Trino connector.

You can download the binary package for installation, 
or you can download the source package for building your own binary package from scratch.

### Install using binary package

After downloading a package file (`.tar.gz`) and its SHA512 checksum file (`.tar.gz.sha512`),
ensure the package file and the checksum file are in the same directory and not renamed.
You can verify the integrity of the package file using the following command:

For example:

```shell
sha512sum -c gravitino-0.8.0-incubating-bin.tar.gz.sha512
```

The output should be something like this:

```
gravitino-0.8.0-incubating-bin.tar.gz: OK
```

After verifying the package file, you can decompress it into your target path.
For example:

```shell
tar xjvf gravitino-0.8.0-incubating-bin.tar.gz
```

You can now proceed to check the [package contents](#package-contents).

### Building the binary package from source

To manually build the binary package from the source code,
you need to either clone the Gravitino GIT repository or download the source package
and then decompress it to your local environment.
You will follow the [how to build guide](./develop/how-to-build.md) to build Gravitino.
After you have run the `./gradlew compileDistribution` command,
you can find the Gravitino binary package contents in the `distribution/package` sub-directory.

### Package contents

The Gravitino binary distribution package contains the following files/directories:

```text
 ├authorizations/                        # Packages for chain authorization and ranger authorization.
 ├bin/
 |  ├gravitino.sh                        # Gravitino server launching scripts.
 |  └ gravitino-iceberg-rest-server.sh   # Gravitino Iceberg REST server launching scripts.
 ├catalogs
 |  ├hadoop/                             # Apache Hadoop catalog dependencies and configurations.
 |  ├hive/                               # Apache Hive catalog dependencies and configurations.
 |  ├jdbc-doris/                         # JDBC doris catalog dependencies and configurations.
 |  ├jdbc-mysql/                         # JDBC MySQL catalog dependencies and configurations.
 |  ├jdbc-postgresql/                    # JDBC PostgreSQL catalog dependencies and configurations.
 |  ├kafka/                              # Apache Kafka PostgreSQL catalog dependencies and configurations.
 |  ├lakehouse-iceberg/                  # Apache Iceberg catalog dependencies and configurations.
 |  └ lakehouse-paimon/                  # Apache Paimon catalog dependencies and configurations.
 ├conf/
 |  ├gravitino.conf                      # Gravitino server and Gravitino Iceberg REST server configuration.
 |  ├gravitino-iceberg-rest-server.conf  # Gravitino server Iceberg REST server configuration.
 |  ├gravitino-env.sh                    # Environment variables.
 |  └log4j2.properties                   # Log4j2 configuration for the Gravitino server and Gravitino Iceberg REST server.
 ├data/                                  # Default data directory for the Gravitino server.
 ├iceberg-rest-server/                   # Gravitino Iceberg REST server package and dependencies libraries.
 ├libs/                                  # Gravitino server dependencies libraries.
 ├licenses/                              # Gravitino server license files.
 ├logs/                                  # Gravitino and Gravitino Iceberg REST server logs. Auto-created when server starts.
 ├scripts/                               # Extra scripts for Gravitino.
 └web/                                   # Web UI for Gravitino.
```

## Set up the Environment

#### Initialize the RDBMS (Optional)

If you want to use the `relational` backend storage, you need to initialize the RDBMS first.
For more details, please check [how to use relational backend storage](./how-to-use-relational-backend-storage.md).

#### Configure the Apache Gravitino server

The Gravitino server configuration file (`conf/gravitino.conf`) contains builtin default settings.
You can configure the Gravitino server by modifying this file.
For details about the configurable options, please check [Gravitino Server configurations](./admin/server-config.md).

#### Configure the server logging

Gravitino uses [Log4j2](https://logging.apache.org/log4j/2.x/) as its logging subsystem.
The Gravitino server logging configuration file is `conf/log4j2.properties`.
You can use this file to configure the server's logging behavior.

#### Configure the Apache Gravitino server environment

The Gravitino server environment configuration file is `conf/gravitino-env.sh`.
Gravitino exposes several environment variables.
You can customize the environment variables by changing this file.

#### Configure Apache Gravitino catalogs

Gravitino supports multiple catalogs backed by different *catalog providers*.
You can configure a catalog by customizing its corresponding configuration file
located in the `catalogs/<catalog-provider>/conf` directory.
The configurations you set here apply to all catalog instances of the same type you create.

For example, if you want to configure the Hive catalog,
you can modify the file `catalogs/hive/conf/hive.conf`.
The detailed configurations are listed in the specific catalog documentation.

:::note
Gravitino checks the catalog configurations in the following order:

1. Catalog `properties` specified in the catalog creation APIs or REST APIs.
1. Catalog configurations specified in the catalog configuration file.

The catalog `properties` can override the settings in the configuration file.
:::

Gravitino supports passing in catalog-specific configurations if you add `gravitino.bypass.`.
For example, if you want to pass in the HMS-specific configuration `hive.metastore.client.capability.check`
to the underlying Hive client in the Hive catalog, add the `gravitino.bypass.` prefix.

Also, Gravitino supports loading catalog-specific configurations from external files.
For example, you can put your own `hive-site.xml` file in the `catalogs/hive/conf` directory,
Gravitino will load it automatically.

## Start Apache Gravitino server

After configuring the Gravitino server, start the Gravitino server by running:

```shell
./bin/gravitino.sh start
```

Alternatively, to start the Gravitino server Web UI, run:

```shell
./bin/gravitino.sh run
```

You can access the Gravitino Web UI at [http://localhost:8090](http://localhost:8090).

To verify that the Gravitino server is running:

```shell
curl -v -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  http://localhost:8090/api/version
```


For information on debugging the Gravitino server,
check the [debugging instructions](./develop/testing.md#debugging-the-apache-gravitino-server).

## Manage Gravitino Iceberg REST server

You can run the [Iceberg REST server](./iceberg-rest-service.md) as standalone server
using the command `./bin/gravitino-iceberg-rest-server.sh start` with configurations provided
in the `./conf/gravitino-iceberg-rest-server.conf` file.

You can also run the Iceberg REST server as an auxiliary service embedded in the Gravitino server
using the command `./bin/gravitino.sh start`.
The configuration options are all consolidated in the `conf/gravitino.conf` file. 

## Install Apache Gravitino using Docker

### Get the Docker image for Apache Gravitino 

Gravitino publishes Docker images to [Docker Hub](https://hub.docker.com/r/apache/gravitino/tags).
You can run the Gravitino Docker image by running:

```shell
docker run -d -i -p 8090:8090 apache/gravitino:<version>
```

You can then access the Gravitino Web UI at `http://localhost:8090`.

You can run the following command to verify if the Gravitino server is running.

```shell
curl -v -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  http://localhost:8090/api/version
```

### Install Apache Gravitino using Docker compose

The Gravitino Docker image published on Docker Hub only contains the Gravitino server with some basic configurations.
If you want to enjoy the whole Gravitino system with other components, you can use the Docker `compose` file.

For more details, check [installing Apache Gravitino playground](./playground/install.md).

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=HowToInstall" alt="" />

