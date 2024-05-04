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
import java.util.Set;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

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
            identifier,
            gravitinoTable,
            sparkIcebergTable,
            propertiesConverter,
            sparkTransformConverter);
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

  @Override
  public Set<TableCapability> capabilities() {
    return sparkBaseTableHelper.capabilities();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return sparkBaseTableHelper.newScanBuilder(options);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return sparkBaseTableHelper.newWriteBuilder(info);
  }

  @Override
  public boolean canDeleteWhere(Filter[] filters) {
    return ((SupportsDelete) sparkBaseTableHelper.getSparkTable()).canDeleteWhere(filters);
  }

  @Override
  public void deleteWhere(Filter[] filters) {
    ((SupportsDelete) sparkBaseTableHelper.getSparkTable()).deleteWhere(filters);
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    return ((SupportsMetadataColumns) sparkBaseTableHelper.getSparkTable()).metadataColumns();
  }

  @Override
  public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
    return ((SupportsRowLevelOperations) sparkBaseTableHelper.getSparkTable())
        .newRowLevelOperationBuilder(info);
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
