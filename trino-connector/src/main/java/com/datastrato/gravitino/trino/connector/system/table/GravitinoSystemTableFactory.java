/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system.table;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.google.common.base.Preconditions;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import java.util.HashMap;
import java.util.Map;

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
