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

# Initial Ranger database in MySQL
sed "s/PLACEHOLDER_RANGER_PASSWORD/${RANGER_PASSWORD}/g" "/tmp/init-mysql.sql.template" > "/tmp/init-mysql.sql"
service mysql start && mysql -uroot < /tmp/init-mysql.sql

# Update Ranger Admin password and setup Ranger Admin
sed -i 's/audit_store=solr/audit_store=DB/g' /opt/ranger-admin/install.properties
sed -i "s/db_password=/db_password=${RANGER_PASSWORD}/g" /opt/ranger-admin/install.properties
sed -i "s/rangerAdmin_password=/rangerAdmin_password=${RANGER_PASSWORD}/g" /opt/ranger-admin/install.properties
sed -i "s/rangerTagsync_password=/rangerTagsync_password=${RANGER_PASSWORD}/g" /opt/ranger-admin/install.properties
sed -i "s/rangerUsersync_password=/rangerUsersync_password=${RANGER_PASSWORD}/g" /opt/ranger-admin/install.properties
sed -i "s/keyadmin_password=/keyadmin_password=${RANGER_PASSWORD}/g" /opt/ranger-admin/install.properties
cd /opt/ranger-admin && /opt/ranger-admin/setup.sh

# Start Ranger Admin
/opt/ranger-admin/ews/ranger-admin-services.sh start

# persist the container
tail -f /dev/null
