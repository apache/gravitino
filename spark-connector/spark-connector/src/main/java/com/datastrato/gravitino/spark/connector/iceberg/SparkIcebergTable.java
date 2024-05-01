/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.utils.SparkBaseTableHelper;
import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.Field;
import java.util.Map;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/**
 * For spark-connector in Iceberg, it explicitly uses SparkTable to identify whether it is an
 * Iceberg table, so the SparkIcebergTable must extend SparkTable.
 */
public class SparkIcebergTable extends SparkTable {

  private SparkBaseTableHelper sparkBaseTableHelper;

  public SparkIcebergTable(
      Identifier identifier,
      Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table sparkIcebergTable,
      TableCatalog sparkIcebergCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter) {
    super(((SparkTable) sparkIcebergTable).table(), !isCacheEnabled(sparkIcebergCatalog));
    this.sparkBaseTableHelper =
        new SparkBaseTableHelper(
            identifier, gravitinoTable, propertiesConverter, sparkTransformConverter);
  }

  @Override
  public String name() {
    return sparkBaseTableHelper.name(true);
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

  private static boolean isCacheEnabled(TableCatalog sparkIcebergCatalog) {
    try {
      SparkCatalog catalog = ((SparkCatalog) sparkIcebergCatalog);
      Field cacheEnabled = catalog.getClass().getDeclaredField("cacheEnabled");
      cacheEnabled.setAccessible(true);
      return cacheEnabled.getBoolean(catalog);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to get cacheEnabled field from SparkCatalog", e);
    }
  }
}
