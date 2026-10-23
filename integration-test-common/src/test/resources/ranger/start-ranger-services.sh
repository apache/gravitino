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

# Gravitino's Ranger test container runs this instead of the image's start script.
# With RANGER_DB_HOST set, Ranger keeps its policies in that MySQL server, the run's shared
# MySQL fixture, which RangerContainer prepares; without it, Ranger uses the MariaDB inside the
# image, as the image's own script does.

set -e

P=/opt/ranger-admin/install.properties

if [ -n "${RANGER_DB_HOST}" ]; then
  # RangerContainer has created an empty database and a rangeradmin user with
  # mysql_native_password, which setup.sh's Connector/J can use without TLS; setup.sh logs in
  # as that user, never as root.
  # db_host has no port: setup.sh also uses it as a MySQL user host.
  sed -i -e "s/^db_host=.*/db_host=${RANGER_DB_HOST}/" \
         -e "s/^db_root_user=.*/db_root_user=rangeradmin/" \
         -e "s/^db_root_password=.*/db_root_password=${RANGER_PASSWORD}/" \
         -e "s/^db_name=.*/db_name=${RANGER_DB_NAME}/" "$P"
else
  sed "s/PLACEHOLDER_RANGER_PASSWORD/${RANGER_PASSWORD}/g" "/tmp/init-mysql.sql.template" > "/tmp/init-mysql.sql"
  service mysql start && mysql -uroot < /tmp/init-mysql.sql
fi

sed -i 's/audit_store=solr/audit_store=DB/g' "$P"
sed -i "s/db_password=/db_password=${RANGER_PASSWORD}/g" "$P"
sed -i "s/rangerAdmin_password=/rangerAdmin_password=${RANGER_PASSWORD}/g" "$P"
sed -i "s/rangerTagsync_password=/rangerTagsync_password=${RANGER_PASSWORD}/g" "$P"
sed -i "s/rangerUsersync_password=/rangerUsersync_password=${RANGER_PASSWORD}/g" "$P"
sed -i "s/keyadmin_password=/keyadmin_password=${RANGER_PASSWORD}/g" "$P"
cd /opt/ranger-admin && /opt/ranger-admin/setup.sh

/opt/ranger-admin/ews/ranger-admin-services.sh start

tail -f /dev/null
