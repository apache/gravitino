---
title: "Installing the Playground"
slug: /installing-the-playground
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Apache Gravitino playground is a bundle of Apache Gravitino Docker runtime environment
comprising of *Apache Hive*, *Apache Hadoop*, *Trino*, *MySQL*, *PostgreSQL*, *Jupyter*,
and a *Apache Gravitino* server.

## Prerequisites

### System environments

- 2 CPU cores
- 8 GB RAM
- 25 GB disk storage
- MacOS or Linux OS (Verified Ubuntu22.04 Ubuntu24.04 AmazonLinux).
- Git (only needed when you are installing the playground from source code).
- Docker, Docker Compose

### Install Docker

Installing Docker and Docker Compose is a requirement for using the playground. 

```shell
sudo apt install docker docker-compose
sudo gpasswd -a $USER docker
newgrp docker
```

### TCP ports used

The playground runs several services.
The TCP ports used by the playground bundle may conflict with your existing services.

| Docker container      | Ports used             |
| --------------------- | ---------------------- |
| playground-gravitino  | 8090 9001              |
| playground-hive       | 3307 19000 19083 60070 |
| playground-mysql      | 13306                  |
| playground-postgresql | 15342                  |
| playground-trino      | 18080                  |
| playground-jupyter    | 18888                  |
| playground-prometheus | 19090                  |
| playground-grafana    | 13000                  |

## Download and launch the playground

You can run the following command to download and launch the playground:

```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/apache/gravitino-playground/HEAD/install.sh)"
```

You can also clone the playground project and then launch the playground:

```shell
git clone git@github.com:apache/gravitino-playground.git
cd gravitino-playground
./playground.sh start
```

Depending on your network and deployment environment, the startup time may take 3-5 minutes.
Once the playground environment has started, you can visit [http://localhost:8090](http://localhost:8090)
in a browser to access the Gravitino Web UI.

## Next Steps

- Learn more about [how to run the playground](./using-the-playground.md)
- Explore the [gravitino-playground project GitHub](https://github.com/apache/gravitino-playground).

