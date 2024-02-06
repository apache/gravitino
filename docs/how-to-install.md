---
title: How to install Gravitino
slug: /how-to-install
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Install Gravitino from scratch

:::note
Gravitino supports running on Java 8, 11, and 17. Make sure you have Java installed and
`JAVA_HOME` configured correctly. To confirm the Java version, run the
`${JAVA_HOME}/bin/java -version` command.
:::

### Get the Gravitino binary distribution package

Before installing Gravitino, make sure you have the Gravitino binary distribution package. You can
download the latest Gravitino binary distribution package from [GitHub](https://github.com/datastrato/gravitino/releases),
or you can build it yourself by following the instructions in [How to Build Gravitino](./how-to-build.md).

  - If you build Gravitino yourself using the `./gradlew compileDistribution` command, you can find the
Gravitino binary distribution package in the `distribution/package` directory.

  - If you build Gravitino yourself using the `./gradlew assembleDistribution` command, you can get the
compressed Gravitino binary distribution package with the name `gravitino-<version>-bin.tar.gz` in the
`distribution` directory with sha256 checksum file `gravitino-<version>-bin.tar.gz.sha256`.

The Gravitino binary distribution package contains the following files:

```text
|── ...
└── distribution/package
    |── bin/gravitino.sh            # Gravitino server Launching scripts.
    |── catalogs
    |   └── hive/                   # Hive catalog dependencies and configurations.
    |   └── lakehouse-iceberg/      # Apache Iceberg catalog dependencies and configurations.
    |   └── jdbc-mysql/             # JDBC MySQL catalog dependencies and configurations.
    |   └── jdbc-postgresql/        # JDBC PostgreSQL catalog dependencies and configurations.
    |── conf/                       # All configurations for Gravitino.
    |   ├── gravitino.conf          # Gravitino server configuration.
    |   ├── gravitino-env.sh        # Environment variables, etc., JAVA_HOME, GRAVITINO_HOME, and more.
    |   └── log4j2.properties       # log4j configuration for the Gravitino server.
    |── libs/                       # Gravitino server dependencies libraries.
    |── logs/                       # Gravitino server logs. Automatically created after the Gravitino server starts.
    └── data/                       # Default directory for the Gravitino server to store data.
```

#### Configure the Gravitino server

The Gravitino server configuration file is `conf/gravitino.conf`. You can configure the Gravitino
server by modifying this file. Basic configurations are already added to this file. All the
configurations are listed in [Gravitino Server Configurations](./gravitino-server-config.md).

#### Configure the Gravitino server log

The Gravitino server log configuration file is `conf/log4j2.properties`. Gravitino uses Log4j2 as
the Logging system. You can [Log4j2](https://logging.apache.org/log4j/2.x/) to
do the log configuration.

#### Configure the Gravitino server environment

The Gravitino server environment configuration file is `conf/gravitino-env.sh`. Gravitino exposes
several environment variables. You can modify them in this file.

#### Configure Gravitino catalogs

Gravitino supports multiple catalogs. You can configure the catalog-level configurations by
modifying the related configuration file in the `catalogs/<catalog-provider>/conf` directory. The
configurations you set here apply to all the catalogs of the same type you create.

For example, if you want to configure the Hive catalog, you can modify the file
`catalogs/hive/conf/hive.conf`. The detailed configurations are listed in the specific catalog
documentation.

:::note
Gravitino takes the catalog configurations in the following order:

1. Catalog `properties` specified in catalog creation API or REST API.
2. Catalog configurations specified in the catalog configuration file.

The catalog `properties` can override the catalog configurations specified in the configuration
file.
:::

Gravitino supports passing in catalog-specific configurations if you add `gravitino.bypass.`. For
example, if you want to pass in the HMS-specific configuration
`hive.metastore.client.capability.check` to the underlying Hive client in the Hive catalog, add the `gravitino.bypass.` prefix to it.

Also, Gravitino supports loading catalog specific configurations from external files. For example,
you can put your own `hive-site.xml` file in the `catalogs/hive/conf` directory, and Gravitino loads
it automatically.

#### Start Gravitino server

After configuring the Gravitino server, start the Gravitino server by running:

```shell
./bin/gravitino.sh start
```

You can access the Gravitino Web UI by typing [http://localhost:8090](http://localhost:8090) in your browser. or you
can run

```shell
curl -v -X GET -H "Accept: application/vnd.gravitino.v1+json" -H "Content-Type: application/json" http://localhost:8090/api/version
```

to make sure Gravitino is running.

:::info
If you need to debug the Gravitino server, enable the `GRAVITINO_DEBUG_OPTS` environment
variable in the `conf/gravitino-env.sh` file. Then create a `Remote JVM Debug`
configuration in `IntelliJ IDEA` and debug `gravitino.server.main`.
:::

## Install Gravitino using Docker

### Get the Gravitino Docker image

Gravitino publishes the Docker image to [Docker Hub](https://hub.docker.com/r/datastrato/gravitino/tags).
Run the Gravitino Docker image by running:

```shell
docker run -d -i -p 8090:8090 datastrato/gravitino:<version>
```

Access the Gravitino Web UI by typing `http://localhost:8090` in your browser, or you
can run

```shell
curl -v -X GET -H "Accept: application/vnd.gravitino.v1+json" -H "Content-Type: application/json" http://localhost:8090/api/version
```

to make sure Gravitino is running.

## Install Gravitino using Docker compose

The published Gravitino Docker image only contains the Gravitino server with basic configurations. If
you want to experience the whole Gravitino system with other components, use the Docker
`compose` file.

For the details, review the
[Gravitino playground repository](https://github.com/datastrato/gravitino-playground) and
[playground example](./how-to-use-the-playground.md).
