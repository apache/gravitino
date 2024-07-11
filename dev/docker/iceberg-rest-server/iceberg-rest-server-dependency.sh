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
script_dir="$(dirname "${BASH_SOURCE-$0}")"
script_dir="$(cd "${script_dir}">/dev/null; pwd)"
gravitino_home="$(cd "${script_dir}/../../..">/dev/null; pwd)"

# Prepare compile Iceberg REST server packages
cd ${gravitino_home}
./gradlew clean assembleIcebergRESTServer -x test

# Removed old packages, Avoid multiple re-executions using the wrong file
rm -rf "${script_dir}/packages"
mkdir -p "${script_dir}/packages"

cd distribution
tar xfz gravitino-iceberg-rest-server-*.tar.gz
cp -r gravitino-iceberg-rest-server*-bin ${script_dir}/packages/iceberg-rest-server

# Keeping the container running at all times
cat <<EOF >> "${script_dir}/packages/iceberg-rest-server/bin/iceberg-rest-server.sh"

# Keeping a process running in the background
tail -f /dev/null
EOF
