/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.sources.Filter;

public class SparkIcebergTable extends SparkBaseTable
    implements SupportsDelete, SupportsMetadataColumns, SupportsRowLevelOperations {

  public SparkIcebergTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkIcebergCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter) {
    super(
        identifier,
        gravitinoTable,
        sparkIcebergCatalog,
        propertiesConverter,
        sparkTransformConverter);
  }

  @Override
  public boolean canDeleteWhere(Filter[] filters) {
    return ((SupportsDelete) getSparkTable()).canDeleteWhere(filters);
  }

  @Override
  public void deleteWhere(Filter[] filters) {
    ((SupportsDelete) getSparkTable()).deleteWhere(filters);
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    return ((SupportsMetadataColumns) getSparkTable()).metadataColumns();
  }

  @Override
  public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
    return ((SupportsRowLevelOperations) getSparkTable()).newRowLevelOperationBuilder(info);
  }

}
