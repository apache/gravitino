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
./gradlew :bundles:gcp-bundle:jar
./gradlew :bundles:aws-bundle:jar

# prepare bundle jar
cd ${iceberg_rest_server_dir}
mkdir -p bundles
cp ${gravitino_home}/bundles/gcp-bundle/build/libs/gravitino-gcp-bundle-*.jar bundles/
cp ${gravitino_home}/bundles/aws-bundle/build/libs/gravitino-aws-bundle-*.jar bundles/

iceberg_gcp_bundle="iceberg-gcp-bundle-1.5.2.jar"
if [ ! -f "bundles/${iceberg_gcp_bundle}" ]; then
  curl -L -s -o bundles/${iceberg_gcp_bundle} https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/1.5.2/${iceberg_gcp_bundle}
fi

iceberg_aws_bundle="iceberg-aws-bundle-1.5.2.jar"
if [ ! -f "bundles/${iceberg_aws_bundle}" ]; then
  curl -L -s -o bundles/${iceberg_aws_bundle} https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/${iceberg_aws_bundle}
fi

# download jdbc driver
curl -L -s -o bundles/sqlite-jdbc-3.42.0.0.jar https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.42.0.0/sqlite-jdbc-3.42.0.0.jar

cp bundles/*jar ${iceberg_rest_server_dir}/packages/gravitino-iceberg-rest-server/libs/

cp start-iceberg-rest-server.sh ${iceberg_rest_server_dir}/packages/gravitino-iceberg-rest-server/bin/
cp rewrite_config.py ${iceberg_rest_server_dir}/packages/gravitino-iceberg-rest-server/bin/

# Keeping the container running at all times
cat <<EOF >> "${iceberg_rest_server_dir}/packages/gravitino-iceberg-rest-server/bin/gravitino-iceberg-rest-server.sh"

# Keeping a process running in the background
tail -f /dev/null
EOF
