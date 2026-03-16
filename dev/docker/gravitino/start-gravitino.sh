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
bin_dir="$(dirname "${BASH_SOURCE-$0}")"
gravitino_dir="$(cd "${bin_dir}/../">/dev/null; pwd)"

cd ${gravitino_dir}

python bin/rewrite_gravitino_server_config.py

# Create soft links for JDBC drivers
jdbc_driver_dir="${gravitino_dir}/jdbc-drivers"

if [ -d "${jdbc_driver_dir}" ]; then
  # Link MySQL driver to catalogs that need it
  mkdir -p "${gravitino_dir}/catalogs/jdbc-mysql/libs"
  mkdir -p "${gravitino_dir}/catalogs/jdbc-doris/libs"
  mkdir -p "${gravitino_dir}/catalogs/jdbc-starrocks/libs"
  mkdir -p "${gravitino_dir}/catalogs/lakehouse-iceberg/libs"
  mkdir -p "${gravitino_dir}/iceberg-rest-server/libs"
  mkdir -p "${gravitino_dir}/libs"
  find "${jdbc_driver_dir}" -name "mysql-connector-java-*.jar" -exec ln -sfv {} "${gravitino_dir}/catalogs/jdbc-mysql/libs/" \;
  find "${jdbc_driver_dir}" -name "mysql-connector-java-*.jar" -exec ln -sfv {} "${gravitino_dir}/catalogs/jdbc-doris/libs/" \;
  find "${jdbc_driver_dir}" -name "mysql-connector-java-*.jar" -exec ln -sfv {} "${gravitino_dir}/catalogs/jdbc-starrocks/libs/" \;
  find "${jdbc_driver_dir}" -name "mysql-connector-java-*.jar" -exec ln -sfv {} "${gravitino_dir}/catalogs/lakehouse-iceberg/libs/" \;
  find "${jdbc_driver_dir}" -name "mysql-connector-java-*.jar" -exec ln -sfv {} "${gravitino_dir}/iceberg-rest-server/libs/" \;
  find "${jdbc_driver_dir}" -name "mysql-connector-java-*.jar" -exec ln -sfv {} "${gravitino_dir}/libs/" \;

  # Link PostgreSQL driver to catalogs that need it
  find "${jdbc_driver_dir}" -name "postgresql-*.jar" -exec ln -sfv {} "${gravitino_dir}/catalogs/jdbc-postgresql/libs/" \;
  find "${jdbc_driver_dir}" -name "postgresql-*.jar" -exec ln -sfv {} "${gravitino_dir}/catalogs/lakehouse-iceberg/libs/" \;
  find "${jdbc_driver_dir}" -name "postgresql-*.jar" -exec ln -sfv {} "${gravitino_dir}/iceberg-rest-server/libs/" \;
  find "${jdbc_driver_dir}" -name "postgresql-*.jar" -exec ln -sfv {} "${gravitino_dir}/libs/" \;
fi

# Create soft links for Iceberg bundle jars
iceberg_bundle_dir="${gravitino_dir}/iceberg-bundles"
lakehouse_iceberg_lib_dir="${gravitino_dir}/catalogs/lakehouse-iceberg/libs"
iceberg_rest_lib_dir="${gravitino_dir}/iceberg-rest-server/libs"

if [ -d "${iceberg_bundle_dir}" ]; then
  mkdir -p "${lakehouse_iceberg_lib_dir}"
  mkdir -p "${iceberg_rest_lib_dir}"
  find ${iceberg_bundle_dir} -name '*.jar' -exec ln -sv {} "${lakehouse_iceberg_lib_dir}" \; -exec ln -sv {} "${iceberg_rest_lib_dir}" \;
fi

JAVA_OPTS+=" -XX:-UseContainerSupport"
export JAVA_OPTS

./bin/gravitino.sh start 
