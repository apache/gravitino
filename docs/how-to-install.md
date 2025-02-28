---
title: How to install Apache Gravitino
slug: /how-to-install
license: "This software is licensed under the Apache License version 2."
---

## Install Apache Gravitino from scratch

### Prerequisites

If you have not installed Java, install it and configure the `JAVA_HOME` environment variable.
After installation, you can run the `${JAVA_HOME}/bin/java -version` command to check the Java version.

:::note
Gravitino supports running on Java 8, 11, and 17.
:::

### Get the Gravitino package

The Gravitino package comprises the Gravitino server and the Gravitino Iceberg REST server.
You can manage these servers independently or run them concurrently on a single server.

1. Download the latest Gravitino package from [GitHub](https://gravitino.apache.org/downloads).

   :::note
   You can also build Gravitino by following the instructions in [How to Build Gravitino](./how-to-build.md).

   - If you build Gravitino using the `./gradlew compileDistribution` command, you can find the Gravitino package in the `distribution/package` directory.

   - If you build Gravitino using the `./gradlew assembleDistribution` command, you can get the compressed Gravitino package with the name `gravitino-<version>-bin.tar.gz` in the `distribution` directory with sha256 checksum file `gravitino-<version>-bin.tar.gz.sha256`.
   :::

2. Extract the Gravitino package and you should have these directories and files:

   ```text
   |── ...
   └── distribution/package
      |── bin/
      |   ├── gravitino.sh                        # Gravitino server Launching scripts.
      |   └── gravitino-iceberg-rest-server.sh    # Gravitino Iceberg REST server Launching scripts.
      |── catalogs
      |   └── hadoop/                             # Apache Hadoop catalog dependencies and configurations.
      |   └── hive/                               # Apache Hive catalog dependencies and configurations.
      |   └── jdbc-doris/                         # JDBC doris catalog dependencies and configurations.
      |   └── jdbc-mysql/                         # JDBC MySQL catalog dependencies and configurations.
      |   └── jdbc-postgresql/                    # JDBC PostgreSQL catalog dependencies and configurations.
      |   └── kafka/                              # Apache Kafka PostgreSQL catalog dependencies and configurations.
      |   └── lakehouse-iceberg/                  # Apache Iceberg catalog dependencies and configurations.
      |   └── lakehouse-paimon/                   # Apache Paimon catalog dependencies and configurations.
      |── conf/                                   # All configurations for Gravitino.
      |   ├── gravitino.conf                      # Gravitino server and Gravitino Iceberg REST server configuration.
      |   ├── gravitino-iceberg-rest-server.conf  # Gravitino server configuration.
      |   ├── gravitino-env.sh                    # Environment variables, etc., JAVA_HOME, GRAVITINO_HOME, and more.
      |   └── log4j2.properties                   # log4j configuration for the Gravitino server and Gravitino Iceberg REST server.
      |── libs/                                   # Gravitino server dependencies libraries.
      |── logs/                                   # Gravitino server and Gravitino Iceberg REST server logs. Automatically created after the server starts.
      |── data/                                   # Default directory for the Gravitino server to store data.
      |── iceberg-rest-server/                    # Gravitino Iceberg REST server package and dependencies libraries.
      └── scripts/                                # Extra scripts for Gravitino.
   ```

### Initialize the RDBMS (Optional)

If you want to use the `relational` backend storage, you need to initialize the RDBMS.
For details on initializing the RDBMS, see [How to use relational backend storage](./how-to-use-relational-backend-storage.md).

### Configure the Gravitino server

The `conf/gravitino.conf` file provides basic configurations for the Gravitino server.
You can update this file to customize the Gravitino server settings as needed.
For a full list of the Gravitino server configurations, see [Gravitino Server Configurations](./gravitino-server-config.md).

### Configure the Gravitino server log

Gravitino uses [Log4j2](https://logging.apache.org/log4j/2.x/) as the logging system.
The logging configuration for the Gravitino server is defined in the `conf/log4j2.properties` file. 
To customize the logging behavior, modify this file as needed.

### Configure the Gravitino server environment variables

Gravitino provides a set of environment variables defined in the conf/gravitino-env.sh file. 
You can configure these environment variables by editing this file.

### Configure the Gravitino catalogs

Gravitino supports multiple catalogs, each with its own configuration.
You can configure catalog-level settings by updating the appropriate configuration file in the `catalogs/<catalog-provider>/conf` directory.
For example, the `catalogs/hive/conf/hive.conf` file contains the configuration options for the Hive catalog.
The settings specified in a catalog configuration file will apply to all catalogs of the same type that you create.
Check the related catalog documentation for detailed information about configuring each catalog,

:::note
Gravitino applies the catalog configurations in the following order of precedence:

1. Catalog `properties` specified in the catalog creation API or REST API.
2. Catalog configurations specified in the catalog configuration file.

The catalog `properties` override the catalog configurations specified in the configuration file.
:::

Gravitino supports passing in catalog-specific configurations by adding the `gravitino.bypass.` prefix to the specific catalog configurations.
For example, you can pass in the HMS-specific configuration `hive.metastore.client.capability.check` to the underlying Hive client in the Hive catalog if you add the `gravitino.bypass.` prefix to the configuration.

Gravitino also supports loading catalog-specific configurations from external files.
You just need to put your `hive-site.xml` file in the `catalogs/hive/conf` directory, and Gravitino will load it automatically.

### Start the Gravitino server

- Start the Gravitino server

  Once you've configured the Gravitino server, use the following command to start it:

   ```shell
   ./bin/gravitino.sh start
   ```

- Start the Gravitino server with the Web UI

  By default, the Gravitino server also provides a Web UI on port `8090`.
  Run the following command to start the Gravitino server with the Web UI:

   ```shell
   ./bin/gravitino.sh run
   ```

After starting the Gravitino server with the Web UI, visit `http://localhost:8090` in your browser to access the Gravitino server through the Web UI.
Or, you can run the following command to verify that the Gravitino server is running:

```shell
curl -v -X GET -H "Accept: application/vnd.gravitino.v1+json" -H "Content-Type: application/json" http://localhost:8090/api/version
```

:::info
If you need to debug the Gravitino server, configure the `GRAVITINO_DEBUG_OPTS` environment variable in the `conf/gravitino-env.sh` file.
Then create a `Remote JVM Debug` configuration in `IntelliJ IDEA` and debug `gravitino.server.main`.
:::

### Start the Gravitino Iceberg REST server

You can run the Iceberg REST server either as a standalone server or as an embedded service embedded in the Gravitino server.

- To start the Iceberg REST server as astandalone server, run the `./bin/gravitino-iceberg-rest-server.sh start` command. The configurations specified in the `./conf/gravitino-iceberg-rest-server.conf` file will be used.
- To start the Iceberg REST server as an embedded service embedded within the Gravitino server, run the `./bin/gravitino.sh start` command. The configurations in the `conf/gravitino.conf` file will be used.

For details about the Gravitino Iceberg REST server, see the [Gravitino Iceberg REST server documentation](./iceberg-rest-service.md).

## Install Apache Gravitino using Docker

Gravitino publishes the Docker image to [Docker Hub](https://hub.docker.com/r/apache/gravitino/tags).
The published Gravitino Docker image only contains the Gravitino server with basic configurations.

### Prerequisites

If you have not installed Docker, download and install it by following the [instructions](https://docs.docker.com/get-started/get-docker/) for your Operating System (OS).

### Steps

1. Start the Gravitino server.

   ```shell
   docker run -d -i -p 8090:8090 apache/gravitino:<version>
   ```

2. Visit `http://localhost:8090` in your browser to access the Gravitino server through the Web UI.
Or, you can run the following command to verify that the Gravitino server is running.

   ```shell
   curl -v -X GET -H "Accept: application/vnd.gravitino.v1+json" -H "Content-Type: application/json" http://localhost:8090/api/version
   ```

## Install Apache Gravitino using Docker Compose

If you want to try out the whole Gravitino system with other components, use Docker Compose.

For details, see [Gravitino playground repository](https://github.com/apache/gravitino-playground) and [playground example](./how-to-use-the-playground.md).
