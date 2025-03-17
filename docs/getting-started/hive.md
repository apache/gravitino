---
title: "Installing Apache Hive"
slug: /getting-started/hive
license: "This software is licensed under the Apache License version 2."
---

To install Apache Hive and Hadoop on Google Cloud Platform manually,
follow [Apache Hive](https://cwiki.apache.org/confluence/display/Hive/) and
[Hadoop](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html).

Installing and configuring Hive can be a little complex.
If you don't already have Hive set up and running, you can use the Docker container
that Datastrato provides to get Gravitino up and running.

Follow these instructions for setting up
[Docker on Ubuntu](https://docs.docker.com/engine/install/ubuntu/).

```shell
sudo docker run --name gravitino-container -d \
  -p 9000:9000 -p 8088:8088 -p 50010:50010 -p 50070:50070 \
  -p 50075:50075 -p 10000:10000 -p 10002:10002 -p 8888:8888 \
  -p 9083:9083 -p 8022:22 \
  apache/gravitino-playground:hive:2.7.3
```

Once Docker is installed, you can start the container with the command:

```shell
sudo docker start gravitino-container
```

<!--TODO: Add some instructions for non-docker environment-->

