/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** May support more capabilities like partition management. */
@Getter
public class SparkHiveTable extends HiveTable implements SparkBaseTable {

  private final Identifier identifier;
  private final Table gravitinoTable;
  private final TableCatalog sparkCatalog;
  private final org.apache.spark.sql.connector.catalog.Table sparkTable;
  private final PropertiesConverter propertiesConverter;

  public SparkHiveTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkHiveCatalog,
      org.apache.spark.sql.connector.catalog.Table sparkHiveTable,
      PropertiesConverter propertiesConverter) {
    super(
        SparkSession.active(),
        ((HiveTable) sparkHiveTable).catalogTable(),
        (HiveTableCatalog) sparkHiveCatalog);
    this.identifier = identifier;
    this.gravitinoTable = gravitinoTable;
    this.sparkCatalog = sparkHiveCatalog;
    this.sparkTable = sparkHiveTable;
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
  public boolean isCaseSensitive() {
    return false;
  }

  // override the scala methods because inherited HiveTable
  @Override
  public boolean canEqual(Object that) {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public Object productElement(int n) {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public int productArity() {
    throw new UnsupportedOperationException("Unsupported operation");
  }
}
