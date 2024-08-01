<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Mac Docker Connector
Because Docker Desktop for Mac does not provide access to container IP from host(macOS).
This can result in host(macOS) and containers not being able to access each other's internal services directly over IPs.
The [mac-docker-connector](https://github.com/wenjunxiao/mac-docker-connector) provides the ability for the macOS host to directly access the docker container IP.
Before running the integration tests, make sure to execute the `dev/docker/tools/mac-docker-connector.sh` script.
> Developing Apache Gravitino in a linux environment does not have this limitation and does not require executing the `mac-docker-connector.sh` script ahead of time.
