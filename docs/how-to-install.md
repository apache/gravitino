---
title: How to install Apache Gravitino
slug: /how-to-install
license: "This software is licensed under the Apache License version 2."
---

## Install Apache Gravitino from scratch

:::note
Apache Gravitino supports running on Java 8, 11, and 17. Make sure you have Java installed and
`JAVA_HOME` configured correctly. To confirm the Java version, run the
`${JAVA_HOME}/bin/java -version` command.
:::

The Gravitino package comprises both the Gravitino server and the Gravitino Iceberg REST server. You can manage these servers independently or run them concurrently on a single server.

### Get the Apache Gravitino binary distribution package

Before installing Gravitino, make sure you have the Gravitino binary distribution package. You can download the latest Gravitino binary distribution package from [GitHub](https://github.com/apache/gravitino/releases).
You can also build it yourself by following the instructions in [How to Build Gravitino](./how-to-build.md).

  - If you build Gravitino yourself using the `./gradlew compileDistribution` command, you can find the Gravitino binary distribution package in the `distribution/package` directory.

  - If you build Gravitino yourself using the `./gradlew assembleDistribution` command, you can get the compressed Gravitino binary distribution package with the name `gravitino-<version>-bin.tar.gz` in the `distribution` directory with sha256 checksum file `gravitino-<version>-bin.tar.gz.sha256`.

The Gravitino binary distribution package contains the following files:

```text
|── ...
└── distribution/package
    |── bin/
    |   ├── gravitino.sh                        # Gravitino server Launching scripts.
    |   └── gravitino-iceberg-rest-server.sh    # Gravitino Iceberg REST server Launching scripts.
    |── catalogs
    |   └── fileset/                            # Fileset catalog dependencies and configurations.
    |   └── hive/                               # Apache Hive catalog dependencies and configurations.
    |   └── jdbc-doris/                         # JDBC doris catalog dependencies and configurations.
    |   └── jdbc-mysql/                         # JDBC MySQL catalog dependencies and configurations.
    |   └── jdbc-postgresql/                    # JDBC PostgreSQL catalog dependencies and configurations.
    |   └── kafka/                              # Apache Kafka PostgreSQL catalog dependencies and configurations.
    |   └── lakehouse-iceberg/                  # Apache Iceberg catalog dependencies and configurations.
    |   └── lakehouse-paimon/                   # Apache Paimon catalog dependencies and configurations.
    |   └── model/                              # Model catalog dependencies and configurations.
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

#### Initialize the RDBMS (Optional)

If you want to use the relational backend storage, you need to initialize the RDBMS first. For the details on initializing the RDBMS, please check [How to use relational backend storage](./how-to-use-relational-backend-storage.md).

#### Configure the Apache Gravitino server

The Gravitino server configuration file is `conf/gravitino.conf`. You can configure the Gravitino server by modifying this file. Basic configurations have already been added to this file. All the configurations are listed in [Gravitino Server Configurations](./gravitino-server-config.md).

#### Configure the Apache Gravitino server log

The Gravitino server log configuration file is `conf/log4j2.properties`. Gravitino uses Log4j2 as the Logging system. You can [Log4j2](https://logging.apache.org/log4j/2.x/) to do the log configuration.

#### Configure the Apache Gravitino server environment

The Gravitino server environment configuration file is `conf/gravitino-env.sh`. Gravitino exposes several environment variables. You can modify them in this file.

#### Configure Apache Gravitino catalogs

Gravitino supports multiple catalogs. You can configure the catalog-level configurations by modifying the related configuration file in the `catalogs/<catalog-provider>/conf` directory. The configurations you set here apply to all the catalogs of the same type you create.

For example, if you want to configure the Hive catalog, you can modify the file `catalogs/hive/conf/hive.conf`. The detailed configurations are listed in the specific catalog documentation.

:::note
Gravitino takes the catalog configurations in the following order:

1. Catalog `properties` specified in catalog creation API or REST API.
2. Catalog configurations specified in the catalog configuration file.

The catalog `properties` can override the catalog configurations specified in the configurationfile.
:::

Gravitino supports passing in catalog-specific configurations if you add `gravitino.bypass.`. For example, if you want to pass in the HMS-specific configuration `hive.metastore.client.capability.check` to the underlying Hive client in the Hive catalog, add the `gravitino.bypass.` prefix.

Also, Gravitino supports loading catalog-specific configurations from external files. For example,you can put your own `hive-site.xml` file in the `catalogs/hive/conf` directory, and Gravitino loads it automatically.

#### Start Apache Gravitino server

After configuring the Gravitino server, start the Gravitino server by running:

```shell
./bin/gravitino.sh start
```

Alternatively, to start the Gravitino server Web UI, please run:

```shell
./bin/gravitino.sh run
```

You can access the Gravitino Web UI by typing [http://localhost:8090](http://localhost:8090) in your browser, or you can run:

```shell
curl -v -X GET -H "Accept: application/vnd.gravitino.v1+json" -H "Content-Type: application/json" http://localhost:8090/api/version
```

to make sure Gravitino is running.

:::info
If you need to debug the Gravitino server, enable the `GRAVITINO_DEBUG_OPTS` environment variable in the `conf/gravitino-env.sh` file. Then create a `Remote JVM Debug` configuration in `IntelliJ IDEA` and debug `gravitino.server.main`.
:::

#### Manage Gravitino Iceberg REST server in Gravitino package

You can run the Iceberg REST server as either a standalone server or as an auxiliary service embedded in the Gravitino server. To start it as a standalone server, use the command `./bin/gravitino-iceberg-rest-server.sh start` with configurations specified in `./conf/gravitino-iceberg-rest-server.conf`. Alternatively, use `./bin/gravitino.sh start` to launch a Gravitino server that integrates both the Iceberg REST service and the Gravitino service, with all configurations centralized in `conf/gravitino.conf`. 

For more detailed information about the Gravitino Iceberg REST server, please refer to [Iceberg REST server document](./iceberg-rest-service.md).

## Install Apache Gravitino using Docker

### Get the Apache Gravitino Docker image

Gravitino publishes the Docker image to [Docker Hub](https://hub.docker.com/r/apache/gravitino/tags).
Run the Gravitino Docker image by running:

```shell
docker run -d -i -p 8090:8090 apache/gravitino:<version>
```

Access the Gravitino Web UI by typing `http://localhost:8090` in your browser, or you
can run

```shell
curl -v -X GET -H "Accept: application/vnd.gravitino.v1+json" -H "Content-Type: application/json" http://localhost:8090/api/version
```

to make sure Gravitino is running.

## Install Apache Gravitino using Docker compose

The published Gravitino Docker image only contains the Gravitino server with a basic configuration. If you want to experience the whole Gravitino system with other components, use the Docker `compose` file.

For the details, review the
[Gravitino playground repository](https://github.com/apache/gravitino-playground) and
[playground example](./how-to-use-the-playground.md).

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=HowToInstall" alt="" />

## Deploy Apache Gravitino on Kubernetes Using Helm Chart

The Apache Gravitino Helm chart provides a way to deploy Gravitino on Kubernetes with fully customizable configurations. 
For detailed installation instructions and configuration options, refer to the [Apache Gravitino Helm Chart](./chart.md).
