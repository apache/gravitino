/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.utils;

import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.spark.connector.ConnectorConstants;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.SparkTypeConverter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Provides schema info from Gravitino, IO from the internal spark table. The specific catalog table
 * could implement more capabilities like SupportsPartitionManagement for Hive table, SupportsIndex
 * for JDBC table, SupportsRowLevelOperations for Iceberg table.
 */
public class SparkBaseTableHelper {

  private Identifier identifier;
  private com.datastrato.gravitino.rel.Table gravitinoTable;
  private Table sparkTable;
  private PropertiesConverter propertiesConverter;
  private SparkTransformConverter sparkTransformConverter;

  public SparkBaseTableHelper(
      Identifier identifier,
      com.datastrato.gravitino.rel.Table gravitinoTable,
      Table sparkTable,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter) {
    this.identifier = identifier;
    this.gravitinoTable = gravitinoTable;
    this.sparkTable = sparkTable;
    this.propertiesConverter = propertiesConverter;
    this.sparkTransformConverter = sparkTransformConverter;
  }

  public String name(boolean isCaseSensitive) {
    return getNormalizedIdentifier(identifier, gravitinoTable.name(), isCaseSensitive);
  }

  public StructType schema() {
    List<StructField> structs =
        Arrays.stream(gravitinoTable.columns())
            .map(
                column -> {
                  String comment = column.comment();
                  Metadata metadata = Metadata.empty();
                  if (comment != null) {
                    metadata =
                        new MetadataBuilder()
                            .putString(ConnectorConstants.COMMENT, comment)
                            .build();
                  }
                  return StructField.apply(
                      column.name(),
                      SparkTypeConverter.toSparkType(column.dataType()),
                      column.nullable(),
                      metadata);
                })
            .collect(Collectors.toList());
    return DataTypes.createStructType(structs);
  }

  public Map<String, String> properties() {
    Map properties = new HashMap();
    if (gravitinoTable.properties() != null) {
      properties.putAll(gravitinoTable.properties());
    }

    properties = propertiesConverter.toSparkTableProperties(properties);

    // Spark will retrieve comment from properties.
    String comment = gravitinoTable.comment();
    if (comment != null) {
      properties.put(ConnectorConstants.COMMENT, comment);
    }
    return properties;
  }

  public Transform[] partitioning() {
    com.datastrato.gravitino.rel.expressions.transforms.Transform[] partitions =
        gravitinoTable.partitioning();
    Distribution distribution = gravitinoTable.distribution();
    SortOrder[] sortOrders = gravitinoTable.sortOrder();
    return sparkTransformConverter.toSparkTransform(partitions, distribution, sortOrders);
  }

  public Set<TableCapability> capabilities() {
    return sparkTable.capabilities();
  }

  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return ((SupportsRead) sparkTable).newScanBuilder(options);
  }

  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return ((SupportsWrite) sparkTable).newWriteBuilder(info);
  }

  public Table getSparkTable() {
    return sparkTable;
  }

  public SparkTransformConverter getSparkTransformConverter() {
    return sparkTransformConverter;
  }

  // The underlying catalogs may not case-sensitive, to keep consistent with the action of SparkSQL,
  // we should return normalized identifiers.
  private String getNormalizedIdentifier(
      Identifier tableIdentifier, String gravitinoTableName, boolean isCaseSensitive) {
    if (tableIdentifier.namespace().length == 0) {
      return gravitinoTableName;
    }

    String databaseName = tableIdentifier.namespace()[0];
    if (!isCaseSensitive) {
      databaseName = databaseName.toLowerCase(Locale.ROOT);
    }

    return String.join(".", databaseName, gravitinoTableName);
  }
}
