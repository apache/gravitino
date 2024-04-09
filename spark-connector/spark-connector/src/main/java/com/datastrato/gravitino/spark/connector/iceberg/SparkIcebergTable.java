/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

@Getter
public class SparkIcebergTable extends SparkTable implements SparkBaseTable, SupportsDelete {

  private final Identifier identifier;
  private final com.datastrato.gravitino.rel.Table gravitinoTable;
  private final TableCatalog sparkCatalog;
  private final org.apache.spark.sql.connector.catalog.Table sparkTable;
  private final PropertiesConverter propertiesConverter;

  public SparkIcebergTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkIcebergCatalog,
      org.apache.spark.sql.connector.catalog.Table sparkIcebergTable,
      PropertiesConverter propertiesConverter) {
    // The purpose of inheritance SparkTable is in order to go through the `isIcebergTable` check in
    // IcebergSparkSqlExtensionsParser.
    // For details, please refer to:
    // https://github.com/apache/iceberg/blob/main/spark/v3.4/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/parser/extensions/IcebergSparkSqlExtensionsParser.scala#L127-L186
    super(((SparkTable) sparkIcebergTable).table(), false);
    this.identifier = identifier;
    this.gravitinoTable = gravitinoTable;
    this.sparkCatalog = sparkIcebergCatalog;
    this.sparkTable = sparkIcebergTable;
    this.propertiesConverter = propertiesConverter;
  }

  @Override
  public String name() {
    return SparkBaseTable.super.name();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return SparkBaseTable.super.schema();
  }

  @Override
  public Map<String, String> properties() {
    return SparkBaseTable.super.properties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return SparkBaseTable.super.capabilities();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return SparkBaseTable.super.newScanBuilder(options);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return SparkBaseTable.super.newWriteBuilder(info);
  }

  @Override
  public Transform[] partitioning() {
    return SparkBaseTable.super.partitioning();
  }

  @Override
  public boolean canDeleteWhere(Filter[] filters) {
    return ((SupportsDelete) getSparkTable()).canDeleteWhere(filters);
  }

  @Override
  public void deleteWhere(Filter[] filters) {
    ((SupportsDelete) getSparkTable()).deleteWhere(filters);
  }
}
