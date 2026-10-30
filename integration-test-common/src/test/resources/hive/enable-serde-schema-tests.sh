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

set -eu

# Patch the configuration inside the prebuilt image before HMS starts.
# Hive 2's compatibility check parses even unchanged SerDe markers as ordinary types.
for config in "${HIVE_TMP_CONF_DIR}/hive-site.xml" "${HIVE_TMP_CONF_DIR}/hive-site-for-sql-base-auth.xml"; do
  awk '
    /<\/configuration>/ {
      print "  <property><name>metastore.storage.schema.reader.impl</name><value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value></property>"
      print "  <property><name>hive.metastore.disallow.incompatible.col.type.changes</name><value>false</value></property>"
    }
    { print }
  ' "$config" > "$config.serde"
  mv "$config.serde" "$config"
done
exec /bin/bash /usr/local/sbin/start.sh
