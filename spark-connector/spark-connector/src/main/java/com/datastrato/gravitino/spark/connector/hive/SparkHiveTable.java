/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.utils.GravitinoTableInfoHelper;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Set;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Keep consistent behavior with the SparkIcebergTable */
public class SparkHiveTable extends HiveTable {

  private GravitinoTableInfoHelper gravitinoTableInfoHelper;
  private org.apache.spark.sql.connector.catalog.Table sparkTable;

  public SparkHiveTable(
      Identifier identifier,
      Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table sparkTable,
      TableCatalog sparkHiveCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter) {
    super(
        SparkSession.active(),
        ((HiveTable) sparkTable).catalogTable(),
        (HiveTableCatalog) sparkHiveCatalog);
    this.gravitinoTableInfoHelper =
        new GravitinoTableInfoHelper(
            identifier, gravitinoTable, propertiesConverter, sparkTransformConverter);
    this.sparkTable = sparkTable;
  }

  @Override
  public String name() {
    return gravitinoTableInfoHelper.name(false);
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return gravitinoTableInfoHelper.schema();
  }

  @Override
  public Map<String, String> properties() {
    return gravitinoTableInfoHelper.properties();
  }

  @Override
  public Transform[] partitioning() {
    return gravitinoTableInfoHelper.partitioning();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return sparkTable.capabilities();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return ((SupportsRead) sparkTable).newScanBuilder(options);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return ((SupportsWrite) sparkTable).newWriteBuilder(info);
  }

  @VisibleForTesting
  public SparkTransformConverter getSparkTransformConverter() {
    return gravitinoTableInfoHelper.getSparkTransformConverter();
  }
}
