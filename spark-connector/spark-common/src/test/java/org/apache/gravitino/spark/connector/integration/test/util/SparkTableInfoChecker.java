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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.integration.test.util.SparkTableInfo.SparkColumnInfo;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.jupiter.api.Assertions;

/**
 * To create an expected SparkTableInfo for verifying the SQL execution result, only the explicitly
 * set fields will be checked.
 */
public class SparkTableInfoChecker {
  private SparkTableInfo expectedTableInfo = new SparkTableInfo();
  private Set<CheckField> checkFields = new LinkedHashSet<>();

  private SparkTableInfoChecker() {}

  public static SparkTableInfoChecker create() {
    return new SparkTableInfoChecker();
  }

  private enum CheckField {
    NAME,
    COLUMN,
    PARTITION,
    BUCKET,
    COMMENT,
    TABLE_PROPERTY,
    METADATA_COLUMN
  }

  public SparkTableInfoChecker withName(String name) {
    this.expectedTableInfo.setTableName(name);
    this.checkFields.add(CheckField.NAME);
    return this;
  }

  public SparkTableInfoChecker withColumns(List<SparkColumnInfo> columns) {
    this.expectedTableInfo.setColumns(columns);
    this.checkFields.add(CheckField.COLUMN);
    return this;
  }

  public SparkTableInfoChecker withBucket(int bucketNum, List<String> bucketColumns) {
    Transform bucketTransform = Expressions.bucket(bucketNum, bucketColumns.toArray(new String[0]));
    this.expectedTableInfo.setBucket(bucketTransform);
    this.checkFields.add(CheckField.BUCKET);
    return this;
  }

  public SparkTableInfoChecker withBucket(
      int bucketNum, List<String> bucketColumns, List<String> sortColumns) {
    Transform sortBucketTransform =
        SparkTransformConverter.createSortBucketTransform(
            bucketNum, bucketColumns.toArray(new String[0]), sortColumns.toArray(new String[0]));
    this.expectedTableInfo.setBucket(sortBucketTransform);
    this.checkFields.add(CheckField.BUCKET);
    return this;
  }

  public SparkTableInfoChecker withIdentifyPartition(List<String> partitionColumns) {
    partitionColumns.forEach(
        columnName -> {
          IdentityTransform identityTransform =
              SparkTransformConverter.createSparkIdentityTransform(columnName);
          this.expectedTableInfo.addPartition(identityTransform);
        });
    this.checkFields.add(CheckField.PARTITION);
    return this;
  }

  public SparkTableInfoChecker withBucketPartition(int bucketNum, List<String> bucketColumns) {
    Transform bucketTransform = Expressions.bucket(bucketNum, bucketColumns.toArray(new String[0]));
    this.expectedTableInfo.addPartition(bucketTransform);
    this.checkFields.add(CheckField.PARTITION);
    return this;
  }

  public SparkTableInfoChecker withHourPartition(String partitionColumn) {
    Transform hourTransform = Expressions.hours(partitionColumn);
    this.expectedTableInfo.addPartition(hourTransform);
    this.checkFields.add(CheckField.PARTITION);
    return this;
  }

  public SparkTableInfoChecker withDayPartition(String partitionColumn) {
    Transform dayTransform = Expressions.days(partitionColumn);
    this.expectedTableInfo.addPartition(dayTransform);
    this.checkFields.add(CheckField.PARTITION);
    return this;
  }

  public SparkTableInfoChecker withMonthPartition(String partitionColumn) {
    Transform monthTransform = Expressions.months(partitionColumn);
    this.expectedTableInfo.addPartition(monthTransform);
    this.checkFields.add(CheckField.PARTITION);
    return this;
  }

  public SparkTableInfoChecker withYearPartition(String partitionColumn) {
    Transform yearTransform = Expressions.years(partitionColumn);
    this.expectedTableInfo.addPartition(yearTransform);
    this.checkFields.add(CheckField.PARTITION);
    return this;
  }

  public SparkTableInfoChecker withTruncatePartition(int width, String partitionColumn) {
    Transform truncateTransform =
        Expressions.apply(
            "truncate", Expressions.literal(width), Expressions.column(partitionColumn));
    this.expectedTableInfo.addPartition(truncateTransform);
    this.checkFields.add(CheckField.PARTITION);
    return this;
  }

  public SparkTableInfoChecker withComment(String comment) {
    this.expectedTableInfo.setComment(comment);
    this.checkFields.add(CheckField.COMMENT);
    return this;
  }

  public SparkTableInfoChecker withTableProperties(Map<String, String> properties) {
    this.expectedTableInfo.setTableProperties(properties);
    this.checkFields.add(CheckField.TABLE_PROPERTY);
    return this;
  }

  public SparkTableInfoChecker withMetadataColumns(SparkMetadataColumnInfo[] metadataColumns) {
    this.expectedTableInfo.setMetadataColumns(metadataColumns);
    this.checkFields.add(CheckField.METADATA_COLUMN);
    return this;
  }

  public void check(SparkTableInfo realTableInfo) {
    checkFields.stream()
        .forEach(
            checkField -> {
              switch (checkField) {
                case NAME:
                  Assertions.assertEquals(
                      expectedTableInfo.getTableName(), realTableInfo.getTableName());
                  break;
                case COLUMN:
                  Assertions.assertEquals(
                      expectedTableInfo.getColumns(), realTableInfo.getColumns());
                  break;
                case PARTITION:
                  Assertions.assertArrayEquals(
                      expectedTableInfo.getPartitions().toArray(),
                      realTableInfo.getPartitions().toArray());
                  break;
                case BUCKET:
                  Assertions.assertEquals(expectedTableInfo.getBucket(), realTableInfo.getBucket());
                  break;
                case METADATA_COLUMN:
                  Assertions.assertEquals(
                      expectedTableInfo.getMetadataColumns().length,
                      realTableInfo.getMetadataColumns().length);
                  for (int i = 0; i < expectedTableInfo.getMetadataColumns().length; i++) {
                    Assertions.assertEquals(
                        expectedTableInfo.getMetadataColumns()[i].name(),
                        realTableInfo.getMetadataColumns()[i].name());
                    Assertions.assertEquals(
                        expectedTableInfo.getMetadataColumns()[i].dataType(),
                        realTableInfo.getMetadataColumns()[i].dataType());
                    Assertions.assertEquals(
                        expectedTableInfo.getMetadataColumns()[i].isNullable(),
                        realTableInfo.getMetadataColumns()[i].isNullable());
                  }
                  break;
                case COMMENT:
                  Assertions.assertEquals(
                      expectedTableInfo.getComment(), realTableInfo.getComment());
                  break;
                case TABLE_PROPERTY:
                  Map<String, String> realTableProperties = realTableInfo.getTableProperties();
                  expectedTableInfo
                      .getTableProperties()
                      .forEach(
                          (k, v) -> {
                            Assertions.assertTrue(
                                realTableProperties.containsKey(k),
                                k + " not exits," + realTableProperties);
                            Assertions.assertEquals(v, realTableProperties.get(k));
                          });
                  break;
                default:
                  Assertions.fail(checkField + " not checked");
                  break;
              }
            });
  }
}
