---
title: "Installing Apache Gravitino Playground"
slug: /getting-started/playground
license: "This software is licensed under the Apache License version 2."
---

Gravitino provides a bundle of Docker images to launch a Gravitino playground,
which includes Apache Hive, Apache Hadoop, Trino, MySQL, PostgreSQL, and Gravitino.
You can use Docker Compose to start them all.

Installing Docker and Docker Compose is a requirement for using the playground. 

```shell
sudo apt install docker docker-compose
sudo gpasswd -a $USER docker
newgrp docker
```

You can install and run all the programs as Docker containers by using the
[gravitino-playground](https://github.com/apache/gravitino-playground).
For details about how to run the playground, see
[how-to-use-the-playground](../how-to-use-the-playground.md)

