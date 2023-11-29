---
title: "Getting Started with Gravitino"
date: 2023-11-20
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---

There are several options for getting started with Gravitino. Installing and configuring Hive and Trino can be a little complex, so if you are unfamiliar with the technologies it would be best to use Docker.

If you want to download and compile Gravitino, on AWS see [Getting started on Amazon Web Services](#getting-started-on-amazon-web-services) and on OSX see [Getting started locally on OSX](#getting-started-locally-on-osx).

If you have your own Gravitino setup and want to use Apache Hive in Docker on AWS see [Installing Apache Hive Trino and Gravitino on AWS](#installing-apache-hive-trino-and-gravitino-on-aws) or locally on OSX see [Installing Apache Hive Trino and Gravitino on OSX](#installing-apache-hive-trino-and-gravitino-on-osx)

If you prefer to get started quickly and use Docker for Gravitino, Apache Hive and Trino on AWS see [Installing Apache Hive on AWS](#installing-apache-hive-on-aws) or locally on OSX see [Installing Apache Hive on OSX](#installing-apache-hive-on-osx).

If you are using AWS and want to access the instance remotely, be sure to read [Accessing Gravitino on AWS externally](#accessing-gravitino-on-aws-externally)


## Getting started on Amazon Web Services

To begin using Gravitino on AWS, follow these steps:

1. In the AWS console, launch a new instance. Select `Ubuntu` as the operating system and `t2.xlarge` as the instance type. Create a key pair named *Gravitino.pem* for SSH access and download it. Allow HTTP and HTTPS traffic, if you want to connect to the instance remotely. Set the Elastic Block Store storage to 20GiB. Leave all other settings as their defaults. Other operating systems and instance types may work, but they have yet to be fully tested.

2. Start the instance and connect to it via SSH using the downloaded .pem file:

    ```shell
    ssh ubuntu@<IP_address> -i ~/Downloads/Gravitino.pem
    ```

   Note you may need to adjust the permissions on your .pem file using `chmod 700` to enable SSH connections.

3. Update the Ubuntu OS to ensure it's up to date:

    ```shell
    sudo apt update
    sudo apt upgrade
    ```

    You may need to reboot the instance for all changes to take effect.

4. Install the required Java Development Kits for Gravitino:

    ```shell
    sudo apt install openjdk-17-jdk-headless
    sudo apt install openjdk-8-jdk-headless
    ```

    List installed Java versions and use Java 8 for now.

    ```shell
    update-java-alternatives --list
    sudo update-java-alternatives --set /usr/lib/jvm/java-1.8.0-openjdk-amd64
    ```

   Verify the Java version with:

    ```shell
    java -version
    ```

   You should see information about OpenJDK 8.

5. Clone the Gravitino source from GitHub:

    ```shell
    git clone git@github.com:datastrato/gravitino.git
    cd gravitino
    ```

   Or, download the latest Gravitino release and extract the source:

    ```shell
    curl -L https://github.com/datastrato/gravitino/releases/download/v0.2.0/gravitino.0.2.0.tar.gz > gravitino.0.2.0.tar.gz
    tar -xvf gravitino.0.2.0.tar.gz
    cd gravitino.0.2.0
    ```

    Or, download, and copy the release to your AWS instance via `scp`.

    ```shell
    scp -i ~/Downloads/Gravitino.pem ~/Downloads/gravitino.zip ubuntu@<IP address>:~
    unzip gravitino.zip
    cd gravitino
    ```

6. Build Gravitino and run tests:

    ```shell
    ./gradlew build
    ```

    You can ignore the tests if you want by appending `-x test`.

7. Build a runnable distribution:

    ```shell
    ./gradlew compileDistribution
    ```

    Again, you can ignore the tests if you want by appending `-x test`.

8. Start Gravitino using the gravitino.sh script:

    ```shell
    distribution/package/bin/gravitino.sh start
    ```

## Getting started locally on OSX

To use Gravitino locally on OSX, follow similar steps:

1. Install the required Java Development Kits using Homebrew:

    ```shell
    brew install openjdk@17
    brew install openjdk@8
    ```

2. Continue with the steps *5* to *8* for AWS to download, compile, and start a Gravitino server.

## Installing Apache Hive on AWS

Installing and configuring Hive can be a little complex. If you don't already have Hive setup and running you can use the Docker container Datastrato provide to get Gravitino up and running.

You can follow the instructions for setting up [Docker on Ubuntu](https://docs.docker.com/engine/install/ubuntu/).

```shell
sudo docker run --name gravitino-container -d -p 9000:9000 -p 8088:8088 -p 50010:50010 -p 50070:50070 -p 50075:50075 -p 10000:10000 -p 10002:10002 -p 8888:8888 -p 9083:9083 -p 8022:22 datastrato/gravitino-ci-hive
```

Once installed, you can start the container with the command:

```shell
sudo docker start gravitino-container
```

## Installing Apache Hive on OSX

The same Docker container operates on OSX.

```shell
docker run --name gravitino-container -d -p 9000:9000 -p 8088:8088 -p 50010:50010 -p 50070:50070 -p 50075:50075 -p 10000:10000 -p 10002:10002 -p 8888:8888 -p 9083:9083 -p 8022:22 datastrato/gravitino-ci-hive
```

## Installing Apache Hive Trino and Gravitino on AWS

Installing Docker and Docker Compose is a requirement to using the playground.

```shell
sudo apt install docker docker-compose
sudo gpasswd -a $USER docker
newgrp docker
```

You can install and run all three programs as Docker containers by using the playground.

```shell
cd ~/gravitino/dev/docker/playground
docker-compose up
```

## Installing Apache Hive Trino and Gravitino on OSX

You can install and run all three programs as Docker containers by using the playground.

```shell
cd ~/gravitino/dev/docker/playground
docker-compose up
```

## Using REST to interact with Gravitino

After starting the Gravitino distribution, issue REST commands to create and modify metadata. While you are using localhost in these examples, run these commands remotely via a host name or IP address once you establish correct access.

1. Create a Metalake

```shell
curl -X POST -H "Content-Type: application/json" -d '{"name":"metalake","comment":"Test metalake"}' http://localhost:8090/api/metalakes
```

Verify the MetaLakes creation

```shell
curl http://localhost:8090/api/metalakes
curl http://localhost:8090/api/metalakes/metalake
```

Note that if you request a Metalake that doesn't exist you get an *NoSuchMetalakeException* error.

```shell
curl http://localhost:8090/api/metalakes/none
```

2. Create a catalog in Hive

First, list the current catalogs to show that no catalogs exist.

```shell
curl http://localhost:8090/api/metalakes/metalake/catalogs
```

Create a new Hive catalog.

```shell
curl -X POST -H "Content-Type: application/json" -d '{"name":"test","comment":"Test catalog","type":"RELATIONAL", "provider":"hive","properties":{"metastore.uris":"thrift://localhost:9083"}}' http://localhost:8090/api/metalakes/metalake/catalogs
```

Verify the creation of the catalog.
```shell
curl http://localhost:8090/api/metalakes/metalake/catalogs
```

Note that the metastore.uris used for the catalog and would need updating if you change your configuration.

## Accessing Gravitino on AWS externally

When deploying Gravitino on AWS, accessing it externally requires some additional configuration due to how AWS networking works.

AWS assigns your instance a public IP address, but Gravitino can't bind to that address. To resolve this, you must find the internal IP address assigned to your AWS instance. You can locate the private IP address in the AWS console, or by running the following command:

```shell
ip a
```

Once you have identified the internal address, edit the Gravitino configuration to bind to that address. Open the file `distribution/package/conf/gravitino.conf` and modify the `gravitino.server.webserver.host` parameter from `127.0.0.1` to your AWS instance's private IP4 address or you can use '0.0.0.0'. '0.0.0.0' in this context means the host's IP address. Restart the Gravitino server for the change to take effect.

```shell
distribution/package/bin/gravitino.sh restart
```

You'll also need to open port 8090 in the security group of your AWS instance to access Gravitino. To access Hive you need to open port 10000 in the security group.

After completing these steps, you should be able to access the Gravitino REST interface from either the command line or a web browser on your local computer. You can also connect to Hive via DBeaver or any other database IDE.
