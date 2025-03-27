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
gravitino_dir="$(dirname "${BASH_SOURCE-$0}")"
gravitino_dir="$(cd "${gravitino_dir}">/dev/null; pwd)"
gravitino_home="$(cd "${gravitino_dir}/../../..">/dev/null; pwd)"

# Build the Gravitino project
${gravitino_home}/gradlew clean build -x test

rm -rf ${gravitino_home}/distribution
# Prepare compile Gravitino packages
${gravitino_home}/gradlew compileDistribution -x test

# Removed old packages, Avoid multiple re-executions using the wrong file
rm -rf "${gravitino_dir}/packages"
mkdir -p "${gravitino_dir}/packages"

cp -r "${gravitino_home}/distribution/package" "${gravitino_dir}/packages/gravitino"

# Copy the Aliyun, AWS, GCP and Azure bundles to the Hadoop catalog libs
cp ${gravitino_home}/bundles/aliyun-bundle/build/libs/*.jar "${gravitino_dir}/packages/gravitino/catalogs/hadoop/libs"
cp ${gravitino_home}/bundles/aws-bundle/build/libs/*.jar "${gravitino_dir}/packages/gravitino/catalogs/hadoop/libs"
cp ${gravitino_home}/bundles/gcp-bundle/build/libs/*.jar "${gravitino_dir}/packages/gravitino/catalogs/hadoop/libs"
cp ${gravitino_home}/bundles/azure-bundle/build/libs/*.jar "${gravitino_dir}/packages/gravitino/catalogs/hadoop/libs"

# Keeping the container running at all times
cat <<EOF >> "${gravitino_dir}/packages/gravitino/bin/gravitino.sh"

# Keeping a process running in the background
tail -f /dev/null
EOF
