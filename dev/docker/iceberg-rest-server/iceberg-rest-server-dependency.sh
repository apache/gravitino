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
#
set -ex
iceberg_rest_server_dir="$(dirname "${BASH_SOURCE-$0}")"
iceberg_rest_server_dir="$(cd "${iceberg_rest_server_dir}">/dev/null; pwd)"
gravitino_home="$(cd "${iceberg_rest_server_dir}/../../..">/dev/null; pwd)"

# Prepare the Iceberg REST server packages
cd ${gravitino_home}
./gradlew clean assembleIcebergRESTServer -x test

# Removed old packages 
rm -rf "${iceberg_rest_server_dir}/packages"
mkdir -p "${iceberg_rest_server_dir}/packages"

cd distribution
tar xfz gravitino-iceberg-rest-server-*.tar.gz
cp -r gravitino-iceberg-rest-server*-bin ${iceberg_rest_server_dir}/packages/gravitino-iceberg-rest-server

cd ${gravitino_home}
./gradlew :bundles:iceberg-gcp-bundle:shadowJar
./gradlew :bundles:iceberg-aws-bundle:shadowJar
./gradlew :bundles:iceberg-azure-bundle:shadowJar
./gradlew :bundles:iceberg-aliyun-bundle:shadowJar

# prepare bundle jar
cd ${iceberg_rest_server_dir}
mkdir -p bundles
find ${gravitino_home}/bundles/iceberg-gcp-bundle/build/libs/ -name 'gravitino-iceberg-gcp-bundle-*.jar' ! -name '*-empty.jar' -exec cp -v {} bundles/ \;
find ${gravitino_home}/bundles/iceberg-aws-bundle/build/libs/ -name 'gravitino-iceberg-aws-bundle-*.jar' ! -name '*-empty.jar' -exec cp -v {} bundles/ \;
find ${gravitino_home}/bundles/iceberg-azure-bundle/build/libs/ -name 'gravitino-iceberg-azure-bundle-*.jar' ! -name '*-empty.jar' -exec cp -v {} bundles/ \;
find ${gravitino_home}/bundles/iceberg-aliyun-bundle/build/libs/ -name 'gravitino-iceberg-aliyun-bundle-*.jar' ! -name '*-empty.jar' -exec cp -v {} bundles/ \;

# download jdbc driver
if [ ! -f "bundles/sqlite-jdbc-3.42.0.0.jar" ]; then
  curl -L -s -o bundles/sqlite-jdbc-3.42.0.0.jar https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.42.0.0/sqlite-jdbc-3.42.0.0.jar
fi

cp bundles/*jar ${iceberg_rest_server_dir}/packages/gravitino-iceberg-rest-server/libs/

cp start-iceberg-rest-server.sh ${iceberg_rest_server_dir}/packages/gravitino-iceberg-rest-server/bin/
cp rewrite_config.py ${iceberg_rest_server_dir}/packages/gravitino-iceberg-rest-server/bin/
