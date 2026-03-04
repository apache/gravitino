/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector.integration.test.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.spark.connector.ConnectorConstants;
import org.apache.gravitino.spark.connector.hive.SparkHiveTable;
import org.apache.gravitino.spark.connector.iceberg.SparkIcebergTable;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTable;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
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
import org.apache.spark.sql.types.StructType;
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
  private List<Transform> partitions = new ArrayList<>();
  private Set<String> partitionColumnNames = new HashSet<>();
  private SparkMetadataColumnInfo[] metadataColumns;

  public SparkTableInfo() {}

  public String getTableName() {
    return tableName;
  }

  public String getTableLocation() {
    return tableProperties.get(TableCatalog.PROP_LOCATION);
  }

  public Map<String, String> getTableProperties() {
    return tableProperties;
  }

  // Include database name and table name
  public String getTableIdentifier() {
    if (StringUtils.isNotBlank(database)) {
      return String.join(".", database, tableName);
    } else {
      return tableName;
    }
  }

  public boolean isPartitionTable() {
    return partitions.size() > 0;
  }

  void setBucket(Transform bucket) {
    Assertions.assertNull(this.bucket, "Should only one distribution");
    this.bucket = bucket;
  }

  void addPartition(Transform partition) {
    if (partition instanceof IdentityTransform) {
      partitionColumnNames.add(((IdentityTransform) partition).reference().fieldNames()[0]);
      this.partitions.add(partition);
    } else if (partition instanceof BucketTransform
        || partition instanceof HoursTransform
        || partition instanceof DaysTransform
        || partition instanceof MonthsTransform
        || partition instanceof YearsTransform
        || (partition instanceof ApplyTransform && "truncate".equalsIgnoreCase(partition.name()))) {
      this.partitions.add(partition);
    } else {
      throw new UnsupportedOperationException("Doesn't support " + partition.name());
    }
  }

  static SparkTableInfo create(Table baseTable) {
    SparkTableInfo sparkTableInfo = new SparkTableInfo();
    String identifier = baseTable.name();
    String[] items = identifier.split("\\.");
    Assertions.assertTrue(
        items.length == 2, "Table name format should be $db.$table, but is: " + identifier);
    sparkTableInfo.tableName = items[1];
    sparkTableInfo.database = items[0];
    sparkTableInfo.columns =
        // using `baseTable.schema()` directly will failed because the method named `schema` is
        // Deprecated in Spark Table interface
        Arrays.stream(getSchema(baseTable).fields())
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
                if (isBucketPartition(baseTable, transform)) {
                  sparkTableInfo.addPartition(transform);
                } else {
                  sparkTableInfo.setBucket(transform);
                }
              } else if (transform instanceof IdentityTransform
                  || transform instanceof HoursTransform
                  || transform instanceof DaysTransform
                  || transform instanceof MonthsTransform
                  || transform instanceof YearsTransform
                  || (transform instanceof ApplyTransform
                      && "truncate".equalsIgnoreCase(transform.name()))) {
                sparkTableInfo.addPartition(transform);
              } else {
                throw new UnsupportedOperationException(
                    "Doesn't support Spark transform: " + transform.name());
              }
            });
    if (baseTable instanceof SupportsMetadataColumns) {
      SupportsMetadataColumns supportsMetadataColumns = (SupportsMetadataColumns) baseTable;
      sparkTableInfo.metadataColumns =
          Arrays.stream(supportsMetadataColumns.metadataColumns())
              .map(
                  metadataColumn ->
                      new SparkMetadataColumnInfo(
                          metadataColumn.name(),
                          metadataColumn.dataType(),
                          metadataColumn.isNullable()))
              .toArray(SparkMetadataColumnInfo[]::new);
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

  private static boolean isBucketPartition(Table baseTable, Transform transform) {
    return baseTable instanceof SparkIcebergTable && !(transform instanceof SortedBucketTransform);
  }

  private static StructType getSchema(Table baseTable) {
    if (baseTable instanceof SparkHiveTable) {
      return ((SparkHiveTable) baseTable).schema();
    } else if (baseTable instanceof SparkIcebergTable) {
      return ((SparkIcebergTable) baseTable).schema();
    } else if (baseTable.getClass().getSimpleName().equals("SparkPaimonTable")) {
      return baseTable.schema();
    } else if (baseTable instanceof SparkJdbcTable) {
      return ((SparkJdbcTable) baseTable).schema();
    } else {
      throw new IllegalArgumentException(
          "Doesn't support Spark table: " + baseTable.getClass().getName());
    }
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
