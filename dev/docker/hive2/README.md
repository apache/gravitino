<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# hadoop2
Build docker image that includes Hadoop2, Hive2

Build Image
===========
./build-docker.sh

Run container
=============
docker run --rm -m -p 8088:8088 -p 50070:50070 -p 50075:50075 -p 10000:10000 -p 10002:10002 -p 8888:8888 -p 9083:9083 -p 8022:22 datastrato/hive2:0.1.0

Login to the server
=============
ssh -p 8022 datastrato@localhost (password: ds123, this is a sudo user)
