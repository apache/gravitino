---
title: "Getting started with Apache Gravitino"
slug: /getting-started
license: "This software is licensed under the Apache License version 2."
---

There are several options for getting started with Apache Gravitino. Installing and configuring Hive and Trino can be a little complex, so if you are unfamiliar with the technologies it would be best to use Docker.

If you want to download and install Gravitino:

  - on AWS, see [Getting started on Amazon Web Services](#getting-started-on-amazon-web-services)
  - Google Cloud Platform, see [Getting started on Google Cloud Platform](#getting-started-on-google-cloud-platform)
  - locally, see [Getting started locally](#getting-started-locally)

If you have your own Apache Gravitino setup and want to use Apache Hive: 

  - on AWS or Google Cloud Platform, see [Installing Apache Hive on AWS or Google Cloud Platform](#installing-apache-hive-on-aws-or-google-cloud-platform)
  - locally, see [Installing Apache Hive locally](#installing-apache-hive-locally)

If you prefer to get started quickly and use Docker for Gravitino, Apache Hive, Trino, and others:

  - on AWS or Google Cloud Platform, see [Installing Gravitino playground on AWS or Google Cloud Platform](#installing-apache-gravitino-playground-on-aws-or-google-cloud-platform)
  - locally, see [Installing Gravitino playground locally](#installing-apache-gravitino-playground-locally)

If you are using AWS and want to access the instance remotely, be sure to read [Accessing Gravitino on AWS externally](#accessing-apache-gravitino-on-aws-externally)

### Index

1. **Installation methods**
   - Explore different installation methods, from using Docker to setting up Gravitino on cloud platforms or locally.

2. **Java Development Kit (JDK)**
   - Ensure you have the required Java Development Kit (JDK) installed to run Gravitino successfully.

3. **Configuring and starting Gravitino**
   - Learn how to configure Gravitino, install it from binary releases or Docker images, and start the Gravitino server.

4. **Getting started on AWS and GCP**
   - Detailed steps for setting up Gravitino on Amazon Web Services (AWS) and Google Cloud Platform (GCP), including instance setup, Java installation, and Gravitino deployment.

5. **Getting started locally**
   - Instructions for using Gravitino locally on macOS or Linux, covering JDK installation and Gravitino setup.

6. **Integrating with Apache Hive**
   - Information on installing and configuring Apache Hive on AWS, GCP, and locally. Docker container options for quick setup are also provided.

7. **Gravitino Playground**
   - Explore a bundled Docker image for a Gravitino playground, incorporating tools like Apache Hive, Apache Hadoop, Trino, MySQL, and PostgreSQL.

8. **Using REST to interact with Gravitino**
   - Examples of interacting with Gravitino via REST commands, demonstrating how to create and modify metadata.

9. **Accessing Gravitino on AWS externally**
   - Guidelines for accessing Gravitino externally when deployed on AWS, including necessary configurations and considerations.

10. **Next steps**
    - Concluding thoughts and suggested next steps for users who have completed the setup.


## Getting started on Amazon Web Services

To begin using Gravitino on AWS, follow these steps:

1. In the AWS console, launch a new instance. Select `Ubuntu` as the operating system and `t2.xlarge` as the instance type. Create a key pair named *Gravitino.pem* for SSH access and download it. Allow HTTP and HTTPS traffic if you want to connect to the instance remotely. Set the Elastic Block Store storage to 20GiB. Leave all other settings at their defaults. Other operating systems and instance types may work, but they have yet to be fully tested.

2. Start the instance and connect to it via SSH using the downloaded .pem file:

    ```shell
    ssh ubuntu@<IP_address> -i ~/Downloads/Gravitino.pem
    ```

   **Note**: you may need to adjust the permissions on your .pem file using `chmod 400` to enable SSH connections.

3. Update the Ubuntu OS to ensure it's up-to-date:

    ```shell
    sudo apt update
    sudo apt upgrade
    ```

    You may need to reboot the instance for all changes to take effect.

4. Install the required Java Development Kit. Gravitino supports running on Java 8,
   11 and 17, so you can install any of them:

    ```shell
    sudo apt install openjdk-<version>-jdk-headless
    ```

   Verify the Java version with:

    ```shell
    java -version
    ```

   You should see information about the OpenJDK version.

5. Install Gravitino on the instance:

   You can install Gravitino from the binary release package or Docker image. Follow
   [how-to-install](./how-to-install.md) to install Gravitino.

   Or you can install Gravitino from scratch. Follow [how-to-build](./how-to-build.md) and [how-to-install](./how-to-install.md).

6. Start Gravitino using the gravitino.sh script:

    ```shell
    <path-to-gravitino>/bin/gravitino.sh start
    ```

## Getting started on Google Cloud Platform

To begin using Gravitino on GCP, follow these steps:

1. In the Google Cloud console, launch a new instance. Select `e2-standard-4` as the instance type and 20 GB for the boot disk size. Allow HTTP and HTTPS traffic if you want to connect to the instance remotely. Leave all other settings as their defaults. Other operating systems and instance types may work, but they have yet to be fully tested.

2. Start the instance and connect to it via the SSH-in-browser tool.

3. Update the Debian OS to ensure it's up-to-date:

    ```shell
    sudo apt update
    sudo apt upgrade
    ```

    You may need to reboot the instance for all changes to take effect.

4. Install the required Java Development Kit. Gravitino supports running on Java 8,
   11 and 17, so you can install any of them:

    ```shell
    wget -O - https://apt.corretto.aws/corretto.key | sudo gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | sudo tee /etc/apt/sources.list.d/corretto.list
    sudo apt-get update
    sudo apt-get install -y java-<version>-amazon-corretto-jdk
    ```

   Verify the Java version with:

    ```shell
    java -version
    ```

   You should see information about the OpenJDK version.

5. Install Gravitino on the instance:

   You can install Gravitino from the binary release package or Docker image. Follow
   [how-to-install](./how-to-install).

   Or you can install Gravitino from scratch. Follow [how-to-build](./how-to-build.md) and [how-to-install](./how-to-install.md).

6. Start Gravitino using the gravitino.sh script:

    ```shell
    <path-to-gravitino>/bin/gravitino.sh start
    ```

## Getting started locally

To use Gravitino locally on macOS or Linux, follow these similar steps:

1. Install the required Java Development Kit. Gravitino supports running on Java 8, so
   11 and 17, you can install any of them. Using [sdkman](https://sdkman.io/), for example:

    ```shell
    sdk install java <version>
    ```

    You can also use different package managers to install JDK, for example,
    [Homebrew](https://brew.sh/) on macOS, `apt` on Ubuntu/Debian, and `yum` on CentOS/RedHat.

2. Install Gravitino:

   You can install Gravitino from the binary release package or Docker image, please follow the
   [how-to-install](./how-to-install.md) to install Gravitino.

   Or, you can install Gravitino from scratch, follow [how-to-build](./how-to-build.md) and [how-to-install](./how-to-install.md).

3. Start Gravitino using the gravitino.sh script:

    ```shell
    <path-to-gravitino>/bin/gravitino.sh start
    ```

## Installing Apache Hive on AWS or Google Cloud Platform

If you already have Apache Hive and Apache Hadoop in your environment, you can ignore this section
and use them with Gravitino.

To install Apache Hive and Hadoop on AWS or Google Cloud Platform manually, follow [Apache Hive](https://cwiki.apache.org/confluence/display/Hive/) and
[Hadoop](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html).

Installing and configuring Hive can be a little complex. If you don't already have Hive set up and running you can use the Docker container Datastrato provides to get Gravitino up and running.

Follow these instructions for setting up [Docker on Ubuntu](https://docs.docker.com/engine/install/ubuntu/).

```shell
sudo docker run --name gravitino-container -d -p 9000:9000 -p 8088:8088 -p 50010:50010 -p 50070:50070 -p 50075:50075 -p 10000:10000 -p 10002:10002 -p 8888:8888 -p 9083:9083 -p 8022:22 apache/gravitino-playground:hive:2.7.3
```

Once Docker is installed, you can start the container with the command:

```shell
sudo docker start gravitino-container
```

## Installing Apache Hive locally

The same steps for installing Hive on AWS or Google Cloud Platform apply when installing it locally. Follow [Installing Apache Hive on AWS or Google Cloud Platform](#installing-apache-hive-on-aws-or-google-cloud-platform).

## Installing Apache Gravitino playground on AWS or Google Cloud Platform

Gravitino provides a bundle of Docker images to launch a Gravitino playground, which
includes Apache Hive, Apache Hadoop, Trino, MySQL, PostgreSQL, and Gravitino. You can use
Docker Compose to start them all.

Installing Docker and Docker Compose is a requirement for using the playground. 

```shell
sudo apt install docker docker-compose
sudo gpasswd -a $USER docker
newgrp docker
```

You can install and run all the programs as Docker containers by using the
[gravitino-playground](https://github.com/apache/gravitino-playground). For details about
how to run the playground, see [how-to-use-the-playground](./how-to-use-the-playground.md)

## Installing Apache Gravitino playground locally

The same steps for installing the playground on AWS or Google Cloud Platform apply when installing it locally. Follow [Installing Gravitino playground on AWS or Google Cloud Platform](#installing-apache-gravitino-playground-on-aws-or-google-cloud-platform).

## Using REST to interact with Apache Gravitino

After starting the Gravitino distribution, issue REST commands to create and modify metadata. While you are using `localhost` in these examples, run these commands remotely via a hostname or IP address once you establish correct access.

1. Create a Metalake:

   ```shell
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" \
   -d '{"name":"metalake","comment":"Test metalake"}' http://localhost:8090/api/metalakes
   ```

   Verify the MetaLake's creation:

   ```shell
   curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" \
   http://localhost:8090/api/metalakes

   curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" \
   http://localhost:8090/api/metalakes/metalake
   ```

   Note that if you request a Metalake that doesn't exist, you get a *NoSuchMetalakeException* error.

   ```shell
   curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" \
   http://localhost:8090/api/metalakes/none
   ```

2. Create a catalog in Hive:

   First, list the current catalogs to verify that no catalogs exist.

   ```shell
   curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" \
   http://localhost:8090/api/metalakes/metalake/catalogs
   ```

   Create a new Hive catalog.

   ```shell
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" \
   -d '{"name":"test","comment":"Test catalog", "type":"RELATIONAL", "provider":"hive", "properties":{"metastore.uris":"thrift://localhost:9083"}}' \
   http://localhost:8090/api/metalakes/metalake/catalogs
   ```

   Verify creation of the catalog.

   ```shell
   curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" \
   http://localhost:8090/api/metalakes/metalake/catalogs
   ```

   Note that the metastore.uris property is used for the Hive catalog and needs updating if you change your configuration.

## Accessing Apache Gravitino on AWS externally

When you deploy Gravitino on AWS, accessing it externally requires some additional configuration due to how AWS networking works.

AWS assigns your instance a public IP address, but Gravitino can't bind to that address. To resolve this, you must find the internal IP address assigned to your AWS instance. You can locate the private IP address in the AWS console, or by running the following command:

```shell
ip a
```

Once you have identified the internal address, edit the Gravitino configuration to bind to that
address. Open the file `<path-to-gravitino>/conf/gravitino.conf` and modify the `gravitino.server.
webserver.host` parameter from `127.0.0.1` to your AWS instance's private IP4 address; or you can use '0.0.0.0'. '0.0.0.0' in this context means the host's IP address. Restart the Gravitino server for the change to take effect.

```shell
<path-to-gravitino>/bin/gravitino.sh restart
```

You'll also need to open port 8090 in the security group of your AWS instance to access Gravitino. To access Hive you need to open port 10000 in the security group.

After completing these steps, you should be able to access the Gravitino REST interface from either the command line or a web browser on your local computer. You can also connect to Hive via DBeaver or any other database IDE.

## Next steps

1. **Explore documentation:**
   - Delve deeper into the Gravitino documentation for advanced features and configuration options.
   - Check out https://gravitino.apache.org/docs/latest

2. **Community engagement:**
   - Join the Gravitino community forums to connect with other users, share experiences, and seek assistance if needed.
   - Check out our GitHub repository: https://github.com/apache/gravitino
   - Check out our Slack channel in ASF Slack: https://the-asf.slack.com
   
3. **Read our blogs:**
   - Check out: https://gravitino.apache.org/blog

4. **Continuous updates:**
   - Stay informed about Gravitino updates and new releases to benefit from the latest features, optimizations, and security       
     enhancements.
   - Check out our Website: https://gravitino.apache.org
  

This document is just the beginning. You're welcome to customize your Gravitino setup based on your requirements and to explore the vast possibilities this powerful tool offers. If you encounter any issues or have questions, you can always connect with the Gravitino community for assistance.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=GettingStarted" alt="" />

