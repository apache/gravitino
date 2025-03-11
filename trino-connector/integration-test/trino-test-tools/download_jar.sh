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

set -e
#!/bin/bash

# Function to download a JAR file if it does not already exist
download_jar() {
  local jar_file_name=$1
  local download_url=$2
  local target_directory=$3

  if [ ! -d "$target_directory" ]; then
    echo "Target directory does not exist. Creating directory: $target_directory"
    mkdir -p "$target_directory"
  fi

  local full_path="$target_directory/$jar_file_name"

  if [ ! -f "$full_path" ]; then
    echo "Downloading $jar_file_name..."
    curl -L -o "$full_path" "$download_url"
    if [ $? -ne 0 ]; then
      echo "Download failed for $jar_file_name"
      exit 1
    fi
    echo "$jar_file_name downloaded successfully."
  else
    echo "$jar_file_name already exists."
  fi
}

# Specific functions for each driver or bundle
download_mysql_jar() {
  download_jar "mysql-connector-java-8.0.26.jar" \
  "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.26/mysql-connector-java-8.0.26.jar" \
  "$GRAVITINO_SERVER_DIR/catalogs/jdbc-mysql/libs"
  cp -rp $GRAVITINO_SERVER_DIR/catalogs/jdbc-mysql/libs/mysql-connector-java-8.0.26.jar $GRAVITINO_SERVER_DIR/catalogs/lakehouse-iceberg/libs
}

download_postgresql_jar() {
  download_jar "postgresql-42.7.0.jar" \
  "https://jdbc.postgresql.org/download/postgresql-42.7.0.jar" \
  "$GRAVITINO_SERVER_DIR/catalogs/jdbc-postgresql/libs"
  cp -rp $GRAVITINO_SERVER_DIR/catalogs/jdbc-postgresql/libs/postgresql-42.7.0.jar $GRAVITINO_SERVER_DIR/catalogs/lakehouse-iceberg/libs
}

download_iceberg_aws_bundle() {
  download_jar "iceberg-aws-bundle-1.6.1.jar" \
  "https://jdbc.postgresql.org/download/iceberg-aws-bundle-1.6.1.jar" \
  "$GRAVITINO_SERVER_DIR/catalogs/lakehouse-iceberg/libs"
}

download_trino-cascading-connector() {
  if [ -d "$GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR" ]; then 
    return
  fi

  path=$GRAVITINO_HOME_DIR/trino-connector/integration-test/build
  download_jar "trino-trino-0.1-SNAPSHOT.tar.gz" \
  "https://github.com/datastrato/trino-cascading-connector/releases/download/v0.0.1/trino-trino-0.1-SNAPSHOT.tar.gz" \
  "$path"

  rm -fr $path/trino-trino
  tar -zxvf $path/trino-trino*.tar.gz -C $path

  if [ ! -d "$GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR" ]; then
      echo "Error: Gravitino Trino connector directory '$GRAVITINO_TRINO_CASCADING_CONNECTOR_DIR' does not exist." >&2
      exit 1
  fi
}

