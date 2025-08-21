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
gravitino_package_dir="${gravitino_dir}/packages/gravitino"
gravitino_staging_dir="${gravitino_package_dir}/staging"
gravitino_iceberg_rest_dir="${gravitino_package_dir}/iceberg-rest-server/libs/"

# Function to download and distribute GCS connector
download_gcs_connector() {
  local gcs_connector_jar="gcs-connector-hadoop2-2.2.18-shaded.jar"
  local gcs_connector_url="https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.18/${gcs_connector_jar}"
  local temp_file="/tmp/${gcs_connector_jar}"

  echo "INFO: Downloading GCS connector: ${gcs_connector_jar}"
  wget -q "${gcs_connector_url}" -O "${temp_file}"

  # Copy to lakehouse-iceberg catalog libs
  mkdir -p "${gravitino_dir}/packages/gravitino/catalogs/lakehouse-iceberg/libs"
  cp "${temp_file}" "${gravitino_dir}/packages/gravitino/catalogs/lakehouse-iceberg/libs/${gcs_connector_jar}"
  echo "INFO: Added GCS connector to lakehouse-iceberg/libs"

  # Copy to iceberg-rest-server libs
  mkdir -p "${gravitino_dir}/packages/gravitino/iceberg-rest-server/libs"
  cp "${temp_file}" "${gravitino_dir}/packages/gravitino/iceberg-rest-server/libs/${gcs_connector_jar}"
  echo "INFO: Added GCS connector to iceberg-rest-server/libs"

  # Clean up temporary file
  rm "${temp_file}"
}

# Build the Gravitino project
${gravitino_home}/gradlew clean build -x test

rm -rf ${gravitino_home}/distribution
# Prepare compile Gravitino packages
${gravitino_home}/gradlew compileDistribution -x test

# Removed old packages, Avoid multiple re-executions using the wrong file
rm -rf "${gravitino_dir}/packages"
mkdir -p "${gravitino_dir}/packages"

cp -r "${gravitino_home}/distribution/package" "${gravitino_package_dir}"
mkdir -p "${gravitino_staging_dir}"

echo "Start to download the jar package"

mysql_driver="mysql-connector-java-8.0.27.jar"
wget "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/$mysql_driver" -O "${gravitino_staging_dir}/${mysql_driver}"
cp "${gravitino_staging_dir}/${mysql_driver}" "${gravitino_package_dir}/catalogs/jdbc-mysql/libs/"
cp "${gravitino_staging_dir}/${mysql_driver}" "${gravitino_package_dir}/catalogs/jdbc-doris/libs/"
cp "${gravitino_staging_dir}/${mysql_driver}" "${gravitino_package_dir}/catalogs/jdbc-starrocks/libs/"
cp "${gravitino_staging_dir}/${mysql_driver}" "${gravitino_package_dir}/catalogs/jdbc-oceanbase/libs/"
cp "${gravitino_staging_dir}/${mysql_driver}" "${gravitino_package_dir}/catalogs/lakehouse-iceberg/libs/"
cp "${gravitino_staging_dir}/${mysql_driver}" "${gravitino_iceberg_rest_dir}"
cp "${gravitino_staging_dir}/${mysql_driver}" "${gravitino_package_dir}/libs/"

pg_driver="postgresql-42.7.0.jar"
wget "https://jdbc.postgresql.org/download/${pg_driver}" -O "${gravitino_staging_dir}/${pg_driver}"
cp "${gravitino_staging_dir}/${pg_driver}" "${gravitino_package_dir}/catalogs/jdbc-postgresql/libs/"
cp "${gravitino_staging_dir}/${pg_driver}" "${gravitino_package_dir}/catalogs/lakehouse-iceberg/libs/"
cp "${gravitino_staging_dir}/${pg_driver}" "${gravitino_iceberg_rest_dir}"

iceberg_version="1.9.2"
iceberg_aws_bundle="iceberg-aws-bundle-${iceberg_version}.jar"
wget "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${iceberg_version}/${iceberg_aws_bundle}" -O "${gravitino_staging_dir}/${iceberg_aws_bundle}"
cp "${gravitino_staging_dir}/${iceberg_aws_bundle}" "${gravitino_iceberg_rest_dir}"

iceberg_gcp_bundle="iceberg-gcp-bundle-${iceberg_version}.jar"
wget "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/${iceberg_version}/${iceberg_gcp_bundle}" -O "${gravitino_staging_dir}/${iceberg_gcp_bundle}"
cp "${gravitino_staging_dir}/${iceberg_gcp_bundle}" "${gravitino_iceberg_rest_dir}"

iceberg_azure_bundle="iceberg-azure-bundle-${iceberg_version}.jar"
wget "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-azure-bundle/${iceberg_version}/${iceberg_azure_bundle}" -O "${gravitino_staging_dir}/${iceberg_azure_bundle}"
cp "${gravitino_staging_dir}/${iceberg_azure_bundle}" "${gravitino_iceberg_rest_dir}"

echo "Finish downloading"

mkdir -p "${gravitino_dir}/packages/gravitino/bin"
cp "${gravitino_dir}/rewrite_gravitino_server_config.py" "${gravitino_dir}/packages/gravitino/bin/"
cp "${gravitino_dir}/start-gravitino.sh" "${gravitino_dir}/packages/gravitino/bin/"

fileset_lib_dir="${gravitino_dir}/packages/gravitino/catalogs/fileset/libs"

# Copy the Aliyun, AWS, GCP and Azure bundles to the Fileset catalog libs
find ${gravitino_home}/bundles/aliyun-bundle/build/libs/ -name 'gravitino-aliyun-bundle-*.jar' ! -name '*-empty.jar' -exec cp -v {} "${fileset_lib_dir}" \;
find ${gravitino_home}/bundles/aws-bundle/build/libs/ -name 'gravitino-aws-*.jar' ! -name '*-empty.jar' -exec cp -v {} "${fileset_lib_dir}" \;
find ${gravitino_home}/bundles/gcp-bundle/build/libs/ -name 'gravitino-gcp-*.jar' ! -name '*-empty.jar' -exec cp -v {} "${fileset_lib_dir}" \;
find ${gravitino_home}/bundles/azure-bundle/build/libs/ -name 'gravitino-azure-*.jar' ! -name '*-empty.jar' -exec cp -v {} "${fileset_lib_dir}" \;

find ${gravitino_home}/bundles/gcp/build/libs/ -name 'gravitino-gcp-*.jar' ! -name '*-empty.jar' -exec cp -v {} "${gravitino_iceberg_rest_dir}" \;
find ${gravitino_home}/bundles/aws/build/libs/ -name 'gravitino-aws-*.jar' ! -name '*-empty.jar' -exec cp -v {} "${gravitino_iceberg_rest_dir}" \;
find ${gravitino_home}/bundles/azure/build/libs/ -name 'gravitino-azure-*.jar' ! -name '*-empty.jar' -exec cp -v {} "${gravitino_iceberg_rest_dir}" \;
find ${gravitino_home}/bundles/aliyun-bundle/build/libs/ -name 'gravitino-aliyun-bundle-*.jar' ! -name '*-empty.jar' -exec cp -v {} "${gravitino_iceberg_rest_dir}" \;

download_gcs_connector

# Keeping the container running at all times
cat <<EOF >> "${gravitino_dir}/packages/gravitino/bin/gravitino.sh"

# Keeping a process running in the background
tail -f /dev/null
EOF
