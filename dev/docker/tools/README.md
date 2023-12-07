<!--
  Copyright 2023 DATASTRATO Pvt Ltd.
  This software is licensed under the Apache License version 2.
-->

# Mac Docker Connector
Because Docker Desktop for Mac does not provide access to container IP from host(macOS).
This can result in host(macOS) and containers not being able to access each other's internal services directly over IPs.
The [mac-docker-connector](https://github.com/wenjunxiao/mac-docker-connector) provides the ability for the macOS host to directly access the docker container IP.
Before running the integration tests, make sure to execute the `dev/docker/tools/mac-docker-connector.sh` script.
> Developing Gravitino in a linux environment does not have this limitation and does not require executing the `mac-docker-connector.sh` script ahead of time.
