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
import java.util.Map;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;

/** This class managed all the system tables */
public class GravitinoSystemTableFactory {

  private final CatalogConnectorManager catalogConnectorManager;
  public static final Map<SchemaTableName, GravitinoSystemTable> SYSTEM_TABLES = new HashMap<>();

  public GravitinoSystemTableFactory(CatalogConnectorManager catalogConnectorManager) {
    this.catalogConnectorManager = catalogConnectorManager;

    registerSystemTables();
  }

  /** Register all the system tables */
  private void registerSystemTables() {
    SYSTEM_TABLES.put(
        GravitinoSystemTableCatalog.TABLE_NAME,
        new GravitinoSystemTableCatalog(catalogConnectorManager));
  }

  public static Page loadPageData(SchemaTableName tableName) {
    Preconditions.checkArgument(SYSTEM_TABLES.containsKey(tableName), "table does not exist");
    return SYSTEM_TABLES.get(tableName).loadPageData();
  }

  public static ConnectorTableMetadata getTableMetaData(SchemaTableName tableName) {
    Preconditions.checkArgument(SYSTEM_TABLES.containsKey(tableName), "table does not exist");
    return SYSTEM_TABLES.get(tableName).getTableMetaData();
  }
}
