/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.catalog;

import javax.ws.rs.NotSupportedException;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** GravitinoHiveCatalog represents a hive catalog in Gravitino */
public class GravitinoHiveCatalog extends BaseCatalog {

  @Override
  public Table createSparkTable(
      Identifier identifier, com.datastrato.gravitino.rel.Table gravitinoTable) {
    throw new NotSupportedException("Not support create spark hive table");
  }

  @Override
  public TableCatalog createSparkCatalog() {
    return new HiveTableCatalog();
  }
}
