#!/bin/bash
#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
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
sed -i 's/check_java_version/#check_java_version/g' /opt/ranger-admin/setup.sh
sed -i 's/#check_java_version()/check_java_version()/g' /opt/ranger-admin/setup.sh
sed -i 's/check_db_connector/#check_db_connector/g' /opt/ranger-admin/setup.sh
sed -i 's/#check_db_connector()/check_db_connector()/g' /opt/ranger-admin/setup.sh
sed -i 's/copy_db_connector/#copy_db_connector/g' /opt/ranger-admin/setup.sh
sed -i 's/#copy_db_connector()/copy_db_connector()/g' /opt/ranger-admin/setup.sh
cd /opt/ranger-admin && /opt/ranger-admin/setup.sh

# Start Ranger Admin
/opt/ranger-admin/ews/ranger-admin-services.sh start

# persist the container
tail -f /dev/null
