/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import lombok.Getter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** May support more capabilities like partition management. */
@Getter
public class SparkHiveTable implements SparkBaseTable {

  private final Identifier identifier;
  private final com.datastrato.gravitino.rel.Table gravitinoTable;
  private final TableCatalog sparkCatalog;
  private final PropertiesConverter propertiesConverter;

  public SparkHiveTable(
      Identifier identifier,
      com.datastrato.gravitino.rel.Table gravitinoTable,
      TableCatalog sparkHiveCatalog,
      PropertiesConverter propertiesConverter) {
    this.identifier = identifier;
    this.gravitinoTable = gravitinoTable;
    this.sparkCatalog = sparkHiveCatalog;
    this.propertiesConverter = propertiesConverter;
  }

  @Override
  public boolean isCaseSensitive() {
    return false;
  }
}
