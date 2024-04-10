/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** May support more capabilities like partition management. */
public class SparkHiveTable extends SparkBaseTable {

  public SparkHiveTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkHiveCatalog,
      PropertiesConverter propertiesConverter) {
    super(identifier, gravitinoTable, sparkHiveCatalog, propertiesConverter);
  }

  @Override
  public boolean isCaseSensitive() {
    return false;
  }
}
