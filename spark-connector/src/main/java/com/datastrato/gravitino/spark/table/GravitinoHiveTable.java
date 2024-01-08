/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.table;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.TypeConverter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class GravitinoHiveTable
    implements org.apache.spark.sql.connector.catalog.Table, SupportsRead, SupportsWrite {
  private Identifier identifier;
  private Table gravitinoTable;
  private HiveTableCatalog hiveTableCatalog;
  private HiveTable lazySparkHiveTable;

  public GravitinoHiveTable(
      Identifier identifier, Table gravitinoTable, HiveTableCatalog hiveTableCatalog) {
    this.identifier = identifier;
    this.gravitinoTable = gravitinoTable;
    this.hiveTableCatalog = hiveTableCatalog;
  }

  private HiveTable getSparkHiveTable() {
    if (lazySparkHiveTable == null) {
      lazySparkHiveTable = (HiveTable) this.hiveTableCatalog.loadTable(identifier);
    }
    return lazySparkHiveTable;
  }

  @Override
  public String name() {
    return identifier.toString();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    List<StructField> structs =
        Arrays.stream(gravitinoTable.columns())
            .map(
                column ->
                    StructField.apply(
                        column.name(),
                        TypeConverter.convert(column.dataType()),
                        column.nullable(),
                        Metadata.empty()))
            .collect(Collectors.toList());
    return StructType$.MODULE$.apply(structs);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return getSparkHiveTable().capabilities();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return getSparkHiveTable().newScanBuilder(options);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return getSparkHiveTable().newWriteBuilder(info);
  }
}
