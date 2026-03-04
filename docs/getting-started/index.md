---
title: "Getting started with Apache Gravitino"
slug: /getting-started/index
license: "This software is licensed under the Apache License version 2."
---

There are several options for getting started with Apache Gravitino.

<!--Docker option-->
Installing and configuring Hive and Trino can be a little complex.
If you are unfamiliar with the technologies, using Docker might be a good choice.
There are pre-packaged containers for Gravitino, Apache Hive, Apache Hadoop,
Trino, MySQL, PostgreSQL, and others.
Check [installing Gravitino playground](./playground.md) for more details.

<!--Build from source-->
This page guides you through the process of downloading and installing Gravitino
from source.

1. [Prepare environment](#environment-preparation)
   - Deploy and run Gravitino on [Amazon Web Service (AWS)](#aws)
   - Deploy and run Gravitino on [Google Compute Platform (GCP)](#gcp)
   - Run Gravitino on [your own machine](#local-workstation)
1. [Install Gravitino](#install-gravitino)
1. [Start Gravitino](#start-gravitino)
1. [Install Apache Hive](#install-apache-hive)
1. [Interact with Apache Gravitino API](#interact-with-apache-gravitino-api)

:::note
If you want to access the instance remotely, be sure to read
[Accessing Gravitino on AWS externally](./aws-remote-access.md).
:::

## Environment preparation

### AWS

To work in an AWS environment, follow these steps:

1. In the AWS console, launch a new instance.
   Select `Ubuntu` as the operating system and `t2.xlarge` as the instance type.
   Create a key pair named *Gravitino.pem* for SSH access and download it.
   Allow HTTP and HTTPS traffic if you want to connect to the instance remotely.
   Set the Elastic Block Store storage to 20GiB.
   Leave all other settings at their defaults.
   Other operating systems and instance types may work but have not been fully tested.

1. Start the instance and connect to it via SSH using the downloaded `.pem` file:

   ```shell
   ssh ubuntu@<IP_address> -i ~/Downloads/Gravitino.pem
   ```

   **Note**: you may need to adjust the permissions on your `.pem` file using
   `chmod 400` to enable SSH connections.

1. Update the Ubuntu OS to ensure it's up-to-date:

   ```shell
   sudo apt update
   sudo apt upgrade
   ```

   <!--TODO: need Red Hat commands?-->
   You may need to reboot the instance for all changes to take effect.

1. Install the Java Development Kit (JDK). Java 17 is supported.

   ```shell
   sudo apt install openjdk-<version>-jdk-headless
   ```

   Verify the Java version with:

   ```shell
   java -version
   ```

   You should see information about the OpenJDK version.

### GCP

To work on the GCP platform, follow these steps:

1. In the Google Cloud console, launch a new instance.
   Select `e2-standard-4` as the instance type and 20 GB for the boot disk size.
   Allow HTTP and HTTPS traffic if you want to connect to the instance remotely.
   Leave all other settings as their defaults.
   Other operating systems and instance types may work, but are not fully tested.

1. Start the instance and connect to it via the SSH-in-browser tool.

1. Update the Debian OS to ensure it's up-to-date:

   ```shell
   sudo apt update
   sudo apt upgrade
   ```

   You may need to reboot the instance for all changes to take effect.

1. Install the Java Development Kit (JDK), Java 17 is supported.

   ```shell
   wget -O - https://apt.corretto.aws/corretto.key | sudo gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg
   echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | sudo tee /etc/apt/sources.list.d/corretto.list
   sudo apt-get update
   sudo apt-get install -y java-<version>-amazon-corretto-jdk
   ```

   Verify the Java version with:

   ```shell
   java -version
   ```

   You should see information about the OpenJDK version.

### Local workstation

To build and install Gravitino locally on a macOS or a Linux workstation,
follow these steps:

1. Install the Java Development Kit (JDK). Java 17 is supported.
   This can be done using [sdkman](https://sdkman.io/), for example:

   ```shell
   sdk install java <version>
   ```

   You can also use different package managers to install JDK, for example,
   [Homebrew](https://brew.sh/) on macOS, `apt` on Ubuntu/Debian, and
   `yum` on CentOS/RedHat.

## Install Gravitino

You can install Gravitino from the binary release packages or the container images.
Follow [how-to-install](../how-to-install.md).

Or you can install Gravitino from scratch.
Follow [how-to-build](../how-to-build.md) and [how-to-install](../how-to-install.md).

## Start Gravitino

Start Gravitino using the `gravitino.sh` script:

```shell
<path-to-gravitino>/bin/gravitino.sh start
```

## Install Apache Hive

If you already have Apache Hive and Apache Hadoop in your environment,
you can skip this step and use the existing service with Gravitino.
Or else, you can follow the [instructions](./hive.md) to install Apache Hive.

## Interact with Apache Gravitino API

After deploying the Gravitino server, you can interact with it
using the RESTful APIs to create and modify metadata.

:::tip
The following examples use `localhost` as the host name.
You may need to revise it based on your environment.
:::

1. Create a Metalake:

   ```shell
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{"name":"my-metalake","comment":"Test metalake"}' \
     http://localhost:8090/api/metalakes
   ```

   Verify the MetaLake has been created:

   ```shell
   curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     http://localhost:8090/api/metalakes

   curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     http://localhost:8090/api/metalakes/my-metalake
   ```

   Note that if you are requesting a Metalake that doesn't exist, you'll get a
   `NoSuchMetalakeException` error.

   ```shell
   curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     http://localhost:8090/api/metalakes/none
   ```

1. Create a catalog in Hive:

   First, list the current catalogs to verify that no catalogs exist.

   ```shell
   curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     http://localhost:8090/api/metalakes/my-metalake/catalogs
   ```

   Create a new Hive catalog.

   ```shell
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{"name":"my-catalog","comment":"Test catalog", "type":"RELATIONAL", "provider":"hive", "properties":{"metastore.uris":"thrift://localhost:9083"}}' \
     http://localhost:8090/api/metalakes/my-metalake/catalogs
   ```

   Verify that the catalog has been created:

   ```shell
   curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     http://localhost:8090/api/metalakes/my-metalake/catalogs
   ```

   :::tip
   The `metastore.uris` property used for the Hive catalog has to
   be adapted to your environment.
   :::

## Next steps

- Delve deeper into the [documentation](https://gravitino.apache.org/docs/latest)
  for advanced features and configuration options.

- Bookmark [Gravitino Website](https://gravitino.apache.org) for updates,
   latest releases, new features, optimizations, and security enhancements.

- Read our [blogs](https://gravitino.apache.org/blog)

- Join the Gravitino community forums to connect with developers and other users,
  for experience sharing and seeking help if needed.
  Questions and comments are all welcome.

  - Join [Gravitino Slack channel](https://the-asf.slack.com)
  - Explore the GitHub repository for [issues](https://github.com/apache/gravitino/issues)
    or [pull requests](https://github.com/apache/gravitino/pulls),
    and pick something you are interested in working on.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=GettingStarted" alt="" />

