#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#args: --auto=none --test_sets_dir=/Users/ice/Documents/workspace/git/graviton/integration-test/trino-cascading-it/testsets --gravitino_uri=http://trino-local.trino-cascading-it.orb.local:8090 --mysql_uri=jdbc:mysql://mysql.trino-cascading-it.orb.local --trino_uri=http://trino-local.trino-cascading-it.orb.local:8080 --params=trino_remote_jdbc_uri,jdbc:trino://trino-remote.trino-cascading-it.orb.local:8080

docker compose up
docker compose down
