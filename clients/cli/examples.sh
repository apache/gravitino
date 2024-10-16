#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#!/bin/bash

# These examples assume you have the Apache Gravitino playground running.

unset GRAVITINO_METALAKE
alias gcli='java -jar clients/cli/build/libs/gravitino-cli-0.7.0-incubating-SNAPSHOT.jar'

# display help
gcli --help

# display version
gcli --version

# metalake details
gcli details

# metalake list
gcli list

# metalake details (all these command are equivalent)
gcli metalake details --name metalake_demo
gcli metalake details --metalake metalake_demo
gcli metalake --command details --metalake metalake_demo
gcli metalake --name metalake_demo details
gcli details --name metalake_demo
gcli details --metalake metalake_demo

# list all catalogs in a metalake 
gcli metalake list --name metalake_demo

# list catalog schema
gcli catalog list --name metalake_demo.catalog_iceberg
gcli catalog list --name metalake_demo.catalog_mysql
gcli catalog list --name metalake_demo.catalog_postgres
gcli catalog list --name metalake_demo.catalog_hive

# list catalog details
gcli catalog details --name metalake_demo.catalog_iceberg
gcli catalog details --name metalake_demo.catalog_mysql
gcli catalog details --name metalake_demo.catalog_postgres
gcli catalog details --name metalake_demo.catalog_hive

# list schema tables
gcli schema list --name metalake_demo.catalog_postgres.hr
gcli schema list --name metalake_demo.catalog_mysql.db
gcli schema list --name metalake_demo.catalog_hive.sales

# list schema details
gcli schema details --name metalake_demo.catalog_postgres.hr
gcli schema details --name metalake_demo.catalog_mysql.db
gcli schema details --name metalake_demo.catalog_hive.sales

# list table details
gcli table list --name metalake_demo.catalog_postgres.hr.departments
gcli table list --name metalake_demo.catalog_mysql.db.iceberg_tables
gcli table list --name metalake_demo.catalog_hive.sales.products

# Metalkes operations
gcli metalake create --name my_metalake --comment "This is my metalake"
gcli metalake delete --name my_metalake 
gcli metalake update --name metalake_demo --rename demo 
gcli metalake update --name demo --rename metalake_demo 
gcli metalake update --name metalake_demo --comment "new comment" 
gcli metalake properties --name metalake_demo
gcli metalake set --name metalake_demo --property test --value value
gcli metalake remove --name metalake_demo --property test

# Catalog operations
gcli catalog create -name metalake_demo.hive --provider hive --metastore thrift://hive-host:9083
gcli catalog create -name metalake_demo.iceberg --provider iceberg --metastore thrift://hive-host:9083 --warehouse hdfs://hdfs-host:9000/user/iceberg/warehouse
gcli catalog create -name metalake_demo.mysql --provider mysql --jdbcurl "jdbc:mysql://mysql-host:3306?useSSL=false" --user user --password password
gcli catalog create -name metalake_demo.postgres --provider postgres --jdbcurl jdbc:postgresql://postgresql-host/mydb --user user --password password -database db
gcli catalog create -name metalake_demo.kafka --provider kafka -bootstrap 127.0.0.1:9092,127.0.0.2:9092
gcli catalog delete -name metalake_demo.hive
gcli catalog delete -name metalake_demo.iceberg
gcli catalog delete -name metalake_demo.mysql
gcli catalog delete -name metalake_demo.postres
gcli catalog delete -name metalake_demo.kafka
gcli catalog update --name metalake_demo.catalog_mysql --rename mysql 
gcli catalog update --name metalake_demo.mysql --rename catalog_mysql 
gcli catalog update --name metalake_demo.catalog_mysql --comment "new comment" 
gcli catalog properties --name metalake_demo.catalog_mysql
gcli catalog set --name metalake_demo.catalog_mysql --property test --value value
gcli catalog remove --name metalake_demo.catalog_mysql --property test

# Schema operations
gcli schema create -name metalake_demo.catalog_postgres.new_db
gcli schema delete -name metalake_demo.catalog_postgres.public
gcli schema properties --name metalake_demo.catalog_postgres.hr
gcli schema set --name metalake_demo.catalog_postgres.hr --property test --value value # not currently supported
gcli schema remove --name metalake_demo.catalog_postgres.hr --property test # not currently supported

# Table operations
gcli table delete -name metalake_demo.catalog_postgres.hr.salaries

# User examples
gcli user create --name metalake_demo --user new_user
gcli user details --name metalake_demo --user new_user
gcli user list --name metalake_demo
gcli user delete --name metalake_demo --user new_user

# Group examples
gcli group create --name metalake_demo --user new_group
gcli group details --name metalake_demo --user new_group
gcli group list --name metalake_demo
gcli group delete --name metalake_demo --user new_group

# Tag examples
gcli tag create --name metalake_demo --tag tagA
gcli tag details --name metalake_demo --tag tagA
gcli tag list --name metalake_demo # all tags in a metalake
gcli tag delete --name metalake_demo --tag tagA
gcli tag update --name metalake_demo --tag tagA --rename new_tag 
gcli tag update --name metalake_demo --tag tagA --comment "new comment" 
gcli tag properties --name metalake_demo --tag tagA 
gcli tag set --name metalake_demo --tag tagA --property color --value green
gcli tag remove --name metalake_demo --tag tagA --property color
gcli tag create --name metalake_demo --tag hr
gcli tag set --name metalake_demo.catalog_postgres.hr --tag hr # tag entity
gcli tag remove --name metalake_demo.catalog_postgres.hr --tag hr # untag entity
gcli tag list --name metalake_demo.catalog_postgres.hr # all tags hr has been taged with

# Role examples
gcli role list --name metalake_demo
gcli role create --name metalake_demo --role admin
gcli role details --name metalake_demo --role admin
gcli role delete --name metalake_demo --role admin

# Exmaples where metalake is set in an evironment variable
export GRAVITINO_METALAKE=metalake_demo

# metalake details
gcli metalake details

# list all catalogs in a metalake 
gcli metalake list

# list catalog schema
gcli catalog list --name catalog_iceberg
gcli catalog list --name catalog_mysql
gcli catalog list --name catalog_postgres
gcli catalog list --name catalog_hive

# list catalog details
gcli catalog details --name catalog_iceberg
gcli catalog details --name catalog_mysql
gcli catalog details --name catalog_postgres
gcli catalog details --name catalog_hive

# list schema tables
gcli schema list --name catalog_postgres.hr
gcli schema list --name catalog_mysql.db
gcli schema list --name catalog_hive.sales

# list schema details
gcli schema details --name catalog_postgres.hr
gcli schema details --name catalog_mysql.db
gcli schema details --name catalog_hive.sales

# list table details
gcli table list --name catalog_postgres.hr.departments
gcli table list --name catalog_mysql.db.iceberg_tables
gcli table list --name catalog_hive.sales.products
