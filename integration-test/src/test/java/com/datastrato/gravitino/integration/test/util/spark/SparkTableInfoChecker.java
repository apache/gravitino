/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.util.spark;

import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo.SparkColumnInfo;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  private List<CheckField> checkFields = new ArrayList<>();

  private SparkTableInfoChecker() {}

  public static SparkTableInfoChecker create() {
    return new SparkTableInfoChecker();
  }

  private enum CheckField {
    NAME,
    COLUMN,
    IDENTITY_PARTITION,
    BUCKET,
    HOUR_PARTITION,
    DAY_PARTITION,
    MONTH_PARTITION,
    YEAR_PARTITION,
    TRUNCATE_PARTITION,
    COMMENT,
    TABLE_PROPERTY,
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

  public SparkTableInfoChecker withIdentifyPartition(List<String> partitionColumns) {
    partitionColumns.forEach(
        columnName -> {
          IdentityTransform identityTransform =
              SparkTransformConverter.createSparkIdentityTransform(columnName);
          this.expectedTableInfo.addPartition(identityTransform);
        });
    this.checkFields.add(CheckField.IDENTITY_PARTITION);
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

  public SparkTableInfoChecker withHourPartition(String partitionColumn) {
    Transform hourTransform = Expressions.hours(partitionColumn);
    this.expectedTableInfo.addHourPartition(hourTransform);
    this.checkFields.add(CheckField.HOUR_PARTITION);
    return this;
  }

  public SparkTableInfoChecker withDayPartition(String partitionColumn) {
    Transform dayTransform = Expressions.days(partitionColumn);
    this.expectedTableInfo.addDayPartition(dayTransform);
    this.checkFields.add(CheckField.DAY_PARTITION);
    return this;
  }

  public SparkTableInfoChecker withMonthPartition(String partitionColumn) {
    Transform monthTransform = Expressions.months(partitionColumn);
    this.expectedTableInfo.addMonthPartition(monthTransform);
    this.checkFields.add(CheckField.MONTH_PARTITION);
    return this;
  }

  public SparkTableInfoChecker withYearPartition(String partitionColumn) {
    Transform yearTransform = Expressions.years(partitionColumn);
    this.expectedTableInfo.addYearPartition(yearTransform);
    this.checkFields.add(CheckField.YEAR_PARTITION);
    return this;
  }

  public SparkTableInfoChecker withTruncatePartition(int width, String partitionColumn) {
    Transform truncateTransform =
        Expressions.apply(
            "truncate", Expressions.literal(width), Expressions.column(partitionColumn));
    this.expectedTableInfo.addTruncatePartition(truncateTransform);
    this.checkFields.add(CheckField.TRUNCATE_PARTITION);
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
                case IDENTITY_PARTITION:
                  Assertions.assertEquals(
                      expectedTableInfo.getPartitions(), realTableInfo.getPartitions());
                  break;
                case BUCKET:
                  Assertions.assertEquals(expectedTableInfo.getBucket(), realTableInfo.getBucket());
                  break;
                case HOUR_PARTITION:
                  Assertions.assertArrayEquals(
                      expectedTableInfo.getHourPartitions().toArray(),
                      realTableInfo.getHourPartitions().toArray());
                  break;
                case DAY_PARTITION:
                  Assertions.assertArrayEquals(
                      expectedTableInfo.getDayPartitions().toArray(),
                      realTableInfo.getDayPartitions().toArray());
                  break;
                case MONTH_PARTITION:
                  Assertions.assertArrayEquals(
                      expectedTableInfo.getMonthPartitions().toArray(),
                      realTableInfo.getMonthPartitions().toArray());
                  break;
                case YEAR_PARTITION:
                  Assertions.assertArrayEquals(
                      expectedTableInfo.getYearPartitions().toArray(),
                      realTableInfo.getYearPartitions().toArray());
                  break;
                case TRUNCATE_PARTITION:
                  Assertions.assertArrayEquals(
                      expectedTableInfo.getTruncatePartitions().toArray(),
                      realTableInfo.getTruncatePartitions().toArray());
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
