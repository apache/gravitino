/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.util.spark;

import com.datastrato.gravitino.spark.connector.ConnectorConstants;
import com.datastrato.gravitino.spark.connector.iceberg.SparkIcebergTable;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.NotSupportedException;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.connector.expressions.ApplyTransform;
import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.DaysTransform;
import org.apache.spark.sql.connector.expressions.HoursTransform;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.MonthsTransform;
import org.apache.spark.sql.connector.expressions.SortedBucketTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.expressions.YearsTransform;
import org.apache.spark.sql.types.DataType;
import org.junit.jupiter.api.Assertions;

/** SparkTableInfo is used to check the result in test. */
@Data
public class SparkTableInfo {
  private String tableName;
  private String database;
  private String comment;
  private List<SparkColumnInfo> columns;
  private Map<String, String> tableProperties;
  private List<String> unknownItems = new ArrayList<>();
  private Transform bucket;
  private Transform hour;
  private Transform day;
  private Transform month;
  private Transform year;
  private Transform truncate;
  private List<Transform> partitions = new ArrayList<>();
  private Set<String> partitionColumnNames = new HashSet<>();
  private SparkMetadataColumn[] metadataColumns;

  public SparkTableInfo() {}

  public String getTableName() {
    return tableName;
  }

  // Include database name and table name
  public String getTableIdentifier() {
    if (StringUtils.isNotBlank(database)) {
      return String.join(".", database, tableName);
    } else {
      return tableName;
    }
  }

  public String getTableLocation() {
    return tableProperties.get(ConnectorConstants.LOCATION);
  }

  public boolean isPartitionTable() {
    return partitions.size() > 0;
  }

  void setBucket(Transform bucket) {
    Assertions.assertNull(this.bucket, "Should only one distribution");
    this.bucket = bucket;
  }

  void setHour(Transform hour) {
    Assertions.assertNull(this.hour, "Should only one distribution");
    this.hour = hour;
  }

  void setDay(Transform day) {
    Assertions.assertNull(this.day, "Should only one distribution");
    this.day = day;
  }

  void setMonth(Transform month) {
    Assertions.assertNull(this.month, "Should only one distribution");
    this.month = month;
  }

  void setYear(Transform year) {
    Assertions.assertNull(this.year, "Should only one distribution");
    this.year = year;
  }

  void setTruncate(Transform truncate) {
    Assertions.assertNull(this.truncate, "Should only one distribution");
    this.truncate = truncate;
  }

  void addPartition(Transform partition) {
    if (partition instanceof IdentityTransform) {
      partitionColumnNames.add(((IdentityTransform) partition).reference().fieldNames()[0]);
    } else {
      throw new NotSupportedException("Doesn't support " + partition.name());
    }
    this.partitions.add(partition);
  }

  static SparkTableInfo create(SparkBaseTable baseTable) {
    SparkTableInfo sparkTableInfo = new SparkTableInfo();
    String identifier = baseTable.name();
    String[] items = identifier.split("\\.");
    Assertions.assertTrue(
        items.length == 2, "Table name format should be $db.$table, but is: " + identifier);
    sparkTableInfo.tableName = items[1];
    sparkTableInfo.database = items[0];
    sparkTableInfo.columns =
        Arrays.stream(baseTable.schema().fields())
            .map(
                sparkField ->
                    new SparkColumnInfo(
                        sparkField.name(),
                        sparkField.dataType(),
                        sparkField.getComment().isDefined() ? sparkField.getComment().get() : null,
                        sparkField.nullable()))
            .collect(Collectors.toList());
    sparkTableInfo.comment = baseTable.properties().remove(ConnectorConstants.COMMENT);
    sparkTableInfo.tableProperties = baseTable.properties();
    Arrays.stream(baseTable.partitioning())
        .forEach(
            transform -> {
              if (transform instanceof BucketTransform
                  || transform instanceof SortedBucketTransform) {
                sparkTableInfo.setBucket(transform);
              } else if (transform instanceof IdentityTransform) {
                sparkTableInfo.addPartition(transform);
              } else if (transform instanceof HoursTransform) {
                sparkTableInfo.setHour(transform);
              } else if (transform instanceof DaysTransform) {
                sparkTableInfo.setDay(transform);
              } else if (transform instanceof MonthsTransform) {
                sparkTableInfo.setMonth(transform);
              } else if (transform instanceof YearsTransform) {
                sparkTableInfo.setYear(transform);
              } else if (transform instanceof ApplyTransform
                  && "truncate".equals(transform.name())) {
                sparkTableInfo.setTruncate(transform);
              } else {
                throw new NotSupportedException(
                    "Doesn't support Spark transform: " + transform.name());
              }
            });
    if (baseTable instanceof SparkIcebergTable) {
      SparkIcebergTable icebergTable = (SparkIcebergTable) baseTable;
      SparkMetadataColumn[] sparkMetadataColumns =
          Arrays.stream(icebergTable.metadataColumns())
              .map(
                  metadataColumn ->
                      new SparkMetadataColumn(
                          metadataColumn.name(),
                          metadataColumn.dataType(),
                          metadataColumn.isNullable()))
              .toArray(SparkMetadataColumn[]::new);
      sparkTableInfo.metadataColumns = sparkMetadataColumns;
    }
    return sparkTableInfo;
  }

  public List<SparkColumnInfo> getUnPartitionedColumns() {
    return columns.stream()
        .filter(column -> !partitionColumnNames.contains(column.name))
        .collect(Collectors.toList());
  }

  public List<SparkColumnInfo> getPartitionedColumns() {
    return columns.stream()
        .filter(column -> partitionColumnNames.contains(column.name))
        .collect(Collectors.toList());
  }

  @Data
  public static class SparkColumnInfo {
    private String name;
    private DataType type;
    private String comment;
    private boolean isNullable;

    private SparkColumnInfo(String name, DataType type, String comment, boolean isNullable) {
      this.name = name;
      this.type = type;
      this.comment = comment;
      this.isNullable = isNullable;
    }

    public static SparkColumnInfo of(String name, DataType type) {
      return of(name, type, null);
    }

    public static SparkColumnInfo of(String name, DataType type, String comment) {
      return new SparkColumnInfo(name, type, comment, true);
    }

    public static SparkColumnInfo of(
        String name, DataType type, String comment, boolean isNullable) {
      return new SparkColumnInfo(name, type, comment, isNullable);
    }
  }
}
