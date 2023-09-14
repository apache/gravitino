<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# hadoop2
Build docker image that includes Hadoop2, Hive2

Build Image
===========
```
./build-docker.sh --platform [all|linux/amd64|linux/arm64] --image {image_name} --tag {tag_name}
```

Run container
=============
```
docker run --rm -d -p 8022:22 -p 8088:8088 -p 8888:8888 -p 9000:9000 -p 9083:9083 -p 10000:10000 -p 10002:10002 -p 50070:50070 -p 50075:50075 datastrato/graviton-ci-hive
```

Login to the server
=============
```
ssh -p 8022 datastrato@localhost (password: ds123, this is a sudo user)
```