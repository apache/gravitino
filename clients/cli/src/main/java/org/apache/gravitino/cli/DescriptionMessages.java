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
