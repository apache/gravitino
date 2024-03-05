/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system;

import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GravitinoSystemTable implements ConnectorPageSource {

  public static Map<SchemaTableName, GravitinoSystemTable> systemTableDefines = new HashMap<>();
  protected static CatalogConnectorManager catalogConnectorManager;

  private boolean isFinished = false;


  static ConnectorPageSource createSystemTablePageSource(SchemaTableName tableName) {
      try {
          return (ConnectorPageSource)systemTableDefines.get(tableName).clone();
      } catch (CloneNotSupportedException e) {
        return null;
      }
  }

  static void registSystemTable(SchemaTableName tableName, GravitinoSystemTableCatalog gravitinoSystemTableCatalog) {
    systemTableDefines.put(tableName, gravitinoSystemTableCatalog);
  }


  static ConnectorTableMetadata getTableMetaData(SchemaTableName tableName) {
    return systemTableDefines.get(tableName).getTableMetaData();
  }



  @Override
  public long getCompletedBytes() {
    return 0;
  }

  @Override
  public long getReadTimeNanos() {
    return 0;
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public Page getNextPage() {
    loadPageData();
    isFinished = true;
    return null;
  }

  @Override
  public long getMemoryUsage() {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }

  public ConnectorTableMetadata getTableMetaData() {
    return null;
  };

  protected Page loadPageData() {
    return null;
  }
}
