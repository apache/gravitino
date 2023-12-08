#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
echo "Start to download the jar package of JDBC"
wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar -o /root/gravitino/catalogs/jdbc-mysql/libs/mysql-connector-java-8.0.27.jar
wget https://jdbc.postgresql.org/download/postgresql-42.7.0.jar -o /root/gravitino/catalogs/jdbc-postgresql/libs/postgresql-42.7.0.jar
echo "Finish downloading"
echo "Start the Gravitino Server"
/bin/bash /root/gravitino/bin/gravitino.sh start