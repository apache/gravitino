/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.cli;

/** Description of the command */
public class DescriptionMessages {
  // -------------------------- Metalake Descriptions --------------------------
  /** Description of details command */
  public static final String METALAKE_DETAILS_DESCRIPTIONS =
      "Get details of a Metalake catalog, %n the example as follows: %n"
          + "# Show details of a metalake%n"
          + "gcli metalake details -m demo_metalake%n"
          + "%n"
          + "# Show metalake's audit information%n"
          + "gcli metalake details -m demo_metalake%n --audit%n";

  /** Description of create command */
  public static final String METALAKE_CREATE_DESCRIPTIONS =
      "Create a new Metalake, %n the example as follows: %n"
          + "# Create a metalake with comment%n"
          + "gcli metalake create --metalake my_metalake --comment 'This is my metalake'%n"
          + "%n"
          + "# Create a metalake with default settings, %n"
          + "gcli metalake create --metalake my_metalake%n";

  /** Description of delete command */
  public static final String METALAKE_DELETE_DESCRIPTIONS =
      "Deleta a metalake, %n the example as follows: %n"
          + "# Delete a metalake%n"
          + "Note:  This is a potentially dangerous command to run and result in data loss.%n"
          + "gcli metalake delete --metalake my_metalake%n";

  /** Description of set command */
  public static final String METALAKE_SET_DESCRIPTIONS =
      "Set properties of a Metalake, %n the example as follows: %n"
          + "# Set properties of a metalake%n"
          + "gcli metalake set --metalake my_metalake --property key1 --value value1%n";

  /** Description of remove command */
  public static final String METALAKE_REMOVE_DESCRIPTIONS =
      "Remove a property of a Metalake, %n the example as follows: %n"
          + "# Remove a property of a metalake%n"
          + "gcli metalake remove --metalake my_metalake --property key1%n";

  /** Description of properties command */
  public static final String METALAKE_PROPERTIES_DESCRIPTIONS =
      "Display the properties of a metalake, %n the example as follows: %n"
          + "# Display the properties of a metalake%n"
          + "gcli metalake properties --metalake my_metalake%n";

  /** Description of update command */
  public static final String METALAKE_UPDATE_DESCRIPTIONS =
      "Update a Metalake, %n the example as follows: %n"
          + "# Rename a metalake%n"
          + "Note:This is a potentially dangerous command to run and may result in unpredictable behaviour.%n"
          + "gcli metalake update --metalake demo_metalake  --rename demo%n"
          + "%n"
          + "# Update a metalake's comment%n"
          + "gcli metalake update --metalake demo_metalake  --comment 'new comment'%n"
          + "%n"
          + "# Enable a metalake%n"
          + "gcli metalake update --metalake demo_metalake  --enable%n"
          + "%n"
          + "# Disable a metalke%n"
          + "gcli metalake update --metalake demo_metalake  --disable%n";

  /** Description of list command */
  public static final String METALAKE_LIST_DESCRIPTIONS =
      "List all Metalakes, %n the example as follows: %n"
          + "# List all Metalakes%n"
          + "gcli metalake list%n";

  // -------------------------- Catalog Descriptions --------------------------
  public static final String CATALOG_DETAILS_DESCRIPTIONS =
      "Get details of a catalog or get audit information for a catalog, %n the example as follows: %n"
          + "# Show a catalog audit information%n"
          + "gcli catalog details -m demo_metalake --name catalog_postgres --audit%n"
          + "%n"
          + "# Show a catalog details%n"
          + "gcli catalog details -m demo_metalake --name catalog_postgres%n";

  public static final String CATALOG_CREATE_DESCRIPTIONS =
      "Create a new catalog in a Metalake, %n the example as follows: %n"
          + "The type of catalog to be created is specified by the `--provider` option. Different catalogs require different properties, for example, a Hive catalog requires a metastore-uri property. %n"
          + "# Create a Hive catalog%n"
          + "gcli catalog create -m demo_metalake --name hive --provider hive --properties metastore.uris=thrift://hive-host:9083%n"
          + "%n"
          + "# Create an Iceberg catalog%n"
          + "gcli catalog create -m demo_metalake  --name iceberg --provider iceberg --properties "
          + "uri=thrift://hive-host:9083,"
          + "catalog-backend=hive,warehouse=hdfs://hdfs-host:9000/user/iceberg/warehouse%n"
          + "%n"
          + "# Create a MySQL catalog%n"
          + "gcli catalog create -m demo_metalake  --name mysql --provider mysql --properties "
          + "jdbc-url=jdbc:mysql://mysql-host:3306?useSSL=false,jdbc-user=user,jdbc-password=password,jdbc-driver=com.mysql.cj.jdbc.Driver"
          + "%n"
          + "# Create a Postgres catalog%n"
          + "gcli catalog create -m demo_metalake  --name postgres --provider postgres --properties "
          + "jdbc-url=jdbc:postgresql://postgresql-host/mydb,jdbc-user=user,jdbc-password=password,jdbc-database=db,jdbc-driver=org.postgresql.Driver%n "
          + "%n"
          + "# Create a Kafka catalog%n"
          + "gcli catalog create -m demo_metalake --name kafka "
          + "--provider kafka --properties bootstrap.servers=127.0.0.1:9092,127.0.0.2:9092%n";

  public static final String CATALOG_DELETE_DESCRIPTIONS =
      "Delete a catalog from a Metalake, %n the example as follows: %n"
          + "# Delete a catalog from a Metalake%n"
          + "gcli catalog delete -m demo_metalake --name catalog_postgres%n";

  public static final String CATALOG_SET_DESCRIPTIONS =
      "Set properties of a catalog in a Metalake, %n the example as follows: %n"
          + "# Set properties of a catalog in a Metalake%n"
          + "gcli catalog set --name catalog_mysql --property test --value value%n";

  public static final String CATALOG_REMOVE_DESCRIPTIONS =
      "Remove a property of a catalog in a Metalake, %n the example as follows: %n"
          + "# Remove a property of a catalog in a Metalake%n"
          + "gcli catalog remove -m demo_metalake --name catalog_mysql --property test%n";

  public static final String CATALOG_PROPERTIES_DESCRIPTIONS =
      "Display a catalog's properties, %n the example as follows: %n"
          + "# Display a catalog's properties%n"
          + "gcli catalog properties -m demo_metalake --name catalog_mysql";

  public static final String CATALOG_LIST_DESCRIPTIONS =
      "Show all catalogs in a metalake, %n the example as follows: %n"
          + "# Show all catalogs in a Metalake%n"
          + "gcli catalog list -m demo_metlake%n";

  public static final String CATALOG_UPDATE_DESCRIPTIONS =
      "Update a catalog in a Metalake, %n the example as follows: %n"
          + "# Rename a catalog%n"
          + "gcli catalog update -m demo_metalake --name catalog_mysql --rename mysql"
          + "%n"
          + "# Update a catalog's comment%n"
          + "gcli catalog update -m demo_metalake --name catalog_mysql --comment 'new comment'%n"
          + "%n"
          + "# Enable a catalog%n"
          + "gcli catalog update -m demo_metalake --name catalog_mysql --enable%n"
          + "%n"
          + "# Disable a catalog%n"
          + "gcli catalog update -m demo_metalake --name catalog_mysql --disable%n";

  // -------------------------- Schema Descriptions --------------------------
  /** Description of details command */
  public static final String SCHEMA_DETAILS_DESCRIPTIONS =
      "Get details of a schema or get audit information for a schema, %n the example as follows: %n"
          + "# Get details of a schema%n"
          + "gcli schema details -m demo_metalake -n catalog.schema%n"
          + "%n"
          + "# Get audit inforamtion of a schema%n"
          + "gcli schema details -m demo_metalake -n catalog.schema --audit%n";

  /** Description of create command */
  public static final String SCHEMA_CREATE_DESCRIPTIONS =
      "Create a new schema in a Metalake catalog, %n the example as follows: %n"
          + "# Create a new schema in a Metalake.catalog%n"
          + "gcli schema create --metalake demo_metalake --name catalog_postgres.new_db";

  /** Description of delete command */
  public static final String SCHEMA_DELETE_DESCRIPTIONS =
      "Delete a schema from a Metalake catalog, %n the example as follows: %n"
          + "# Delete a schema from a Metalake.catalog%n"
          + "gcli schema delete --metalake demo_metalake --name catalog_postgres.old_db%n"
          + "%n"
          + "# Force delete a schema from a Metalake.catalog"
          + "gcli schema delete --metalake demo_metalake --name catalog_postgres.old_db --force%n";

  /** Description of set command */
  public static final String SCHEMA_SET_DESCRIPTIONS =
      "Set properties of a schema in a Metalake catalog, %n the example as follows: %n"
          + "# Set properties of a schema in a Metalake.catalog%n"
          + "gcli schema set --metalake demo_metalake --name catalog_postgres.old_db --property key1 --value value1%n";

  /** Description of remove command */
  public static final String SCHEMA_REMOVE_DESCRIPTIONS =
      "Remove a property of a schema in a Metalake catalog, %n the example as follows: %n"
          + "# Remove a property of a schema in a Metalake.catalog%n"
          + "gcli schema remove --metalake demo_metalake --name catalog_postgres.old_db --property key1%n";

  /** Description of properties command */
  public static final String SCHEMA_PROPERTIES_DESCRIPTIONS =
      "List all properties of a schema in a Metalake catalog, %n the example as follows: %n"
          + "# List all properties of a schema in a Metalake.catalog%n"
          + "gcli schema properties --metalake demo_metalake --name catalog_postgres.hr";

  /** Description of list command */
  public static final String SCHEMA_LIST_DESCRIPTIONS =
      "List all schemas in a Metalake catalog, %n the example as "
          + "follows: %n"
          + "# List all schemas in a Metalake.catalog, use plain format%n"
          + "gcli schema list --metalake demo_metalake --name catalog_postgres%n"
          + "%n"
          + "# List all schemas in a Metalake.catalog, use table format%n"
          + "gcli schema list --metalake demo_metalake --name catalog_postgres --output table%n";
}
