/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.utils.SparkBaseTableHelper;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/** Keep consistent behavior with the SparkIcebergTable */
public class SparkHiveTable extends HiveTable {

  private SparkBaseTableHelper sparkBaseTableHelper;

  public SparkHiveTable(
      Identifier identifier,
      Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table sparkHiveTable,
      TableCatalog sparkHiveCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter) {
    super(
        SparkSession.active(),
        ((HiveTable) sparkHiveTable).catalogTable(),
        (HiveTableCatalog) sparkHiveCatalog);
    this.sparkBaseTableHelper =
        new SparkBaseTableHelper(
            identifier, gravitinoTable, propertiesConverter, sparkTransformConverter);
  }

  @Override
  public String name() {
    return sparkBaseTableHelper.name(false);
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return sparkBaseTableHelper.schema();
  }

  @Override
  public Map<String, String> properties() {
    return sparkBaseTableHelper.properties();
  }

  @Override
  public Transform[] partitioning() {
    return sparkBaseTableHelper.partitioning();
  }

  @VisibleForTesting
  public SparkTransformConverter getSparkTransformConverter() {
    return sparkBaseTableHelper.getSparkTransformConverter();
  }
}
