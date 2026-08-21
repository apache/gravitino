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
package org.apache.gravitino.trino.connector.system.table;

import com.google.common.base.Preconditions;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;

/** This class managed all the system tables */
public class GravitinoSystemTableFactory {

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  // Per instance, not static: the tables are bound to one CatalogConnectorManager, and only the
  // manager on the coordinator runs the load loop that fills in the registration state. A shared
  // registry would let any other connector in the JVM take it over.
  private final Map<SchemaTableName, GravitinoSystemTable> systemTables = new HashMap<>();

  /**
   * Constructs a new GravitinoSystemTableFactory.
   *
   * @param catalogConnectorManager the manager for catalog connectors
   * @param metalake the metalake this connector is configured with; the tables only report on it,
   *     so that two entry catalogs pointed at different metalakes do not report each other's state
   */
  public GravitinoSystemTableFactory(
      CatalogConnectorManager catalogConnectorManager, String metalake) {
    this.catalogConnectorManager = catalogConnectorManager;
    this.metalake = metalake;

    registerSystemTables();
  }

  /** Register all the system tables */
  private void registerSystemTables() {
    systemTables.put(
        GravitinoSystemTableCatalog.TABLE_NAME,
        new GravitinoSystemTableCatalog(catalogConnectorManager, metalake));
    systemTables.put(
        GravitinoSystemTableCatalogStatus.TABLE_NAME,
        new GravitinoSystemTableCatalogStatus(catalogConnectorManager, metalake));
    systemTables.put(
        GravitinoSystemTableLoadStatus.TABLE_NAME,
        new GravitinoSystemTableLoadStatus(catalogConnectorManager, metalake));
  }

  /**
   * Loads the page data for a given system table.
   *
   * @param tableName the schema-qualified name of the table
   * @return the page containing the table's data
   * @throws IllegalArgumentException if the table does not exist
   */
  public Page loadPageData(SchemaTableName tableName) {
    Preconditions.checkArgument(systemTables.containsKey(tableName), "table does not exist");
    return systemTables.get(tableName).loadPageData();
  }

  /**
   * Gets the table metadata for a given system table.
   *
   * @param tableName the schema-qualified name of the table
   * @return the table metadata
   * @throws IllegalArgumentException if the table does not exist
   */
  public ConnectorTableMetadata getTableMetaData(SchemaTableName tableName) {
    Preconditions.checkArgument(systemTables.containsKey(tableName), "table does not exist");
    return systemTables.get(tableName).getTableMetaData();
  }

  /**
   * Lists the names of every registered system table.
   *
   * @return the schema-qualified table names
   */
  public List<SchemaTableName> listTableNames() {
    return List.copyOf(systemTables.keySet());
  }

  /**
   * Checks whether a system table is registered.
   *
   * @param tableName the schema-qualified name of the table
   * @return true if the table exists, false otherwise
   */
  public boolean tableExists(SchemaTableName tableName) {
    return systemTables.containsKey(tableName);
  }
}
