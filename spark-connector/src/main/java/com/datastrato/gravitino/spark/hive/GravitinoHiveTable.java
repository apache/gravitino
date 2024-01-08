/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.hive;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.PropertiesConverter;
import com.datastrato.gravitino.spark.table.GravitinoBaseTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** May support more capabilities like partition management. */
public class GravitinoHiveTable extends GravitinoBaseTable {
  public GravitinoHiveTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter) {
    super(identifier, gravitinoTable, sparkCatalog, propertiesConverter);
  }
}
