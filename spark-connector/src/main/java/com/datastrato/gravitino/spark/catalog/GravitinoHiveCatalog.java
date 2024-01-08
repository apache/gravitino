/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.catalog;

import com.datastrato.gravitino.spark.table.GravitinoHiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** GravitinoHiveCatalog represents a hive catalog in Gravitino */
public class GravitinoHiveCatalog extends GravitinoCatalog {

  @Override
  public Table createGravitinoTable(
      Identifier identifier, com.datastrato.gravitino.rel.Table gravitinoTable) {
    return new GravitinoHiveTable(identifier, gravitinoTable, (HiveTableCatalog) sparkCatalog);
  }

  @Override
  public TableCatalog createSparkCatalog() {
    return new HiveTableCatalog();
  }
}
