/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.util.spark;

import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo.SparkColumnInfo;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import java.util.ArrayList;
import java.util.List;
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
    PARTITION,
    BUCKET,
    HOUR,
    DAY,
    MONTH,
    YEAR,
    TRUNCATE,
    COMMENT,
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
    this.checkFields.add(CheckField.PARTITION);
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

  public SparkTableInfoChecker withHour(List<String> partitionColumns) {
    Transform hourTransform = Expressions.hours(partitionColumns.get(0));
    this.expectedTableInfo.setHour(hourTransform);
    this.checkFields.add(CheckField.HOUR);
    return this;
  }

  public SparkTableInfoChecker withDay(List<String> partitionColumns) {
    Transform dayTransform = Expressions.days(partitionColumns.get(0));
    this.expectedTableInfo.setDay(dayTransform);
    this.checkFields.add(CheckField.DAY);
    return this;
  }

  public SparkTableInfoChecker withMonth(List<String> partitionColumns) {
    Transform monthTransform = Expressions.months(partitionColumns.get(0));
    this.expectedTableInfo.setMonth(monthTransform);
    this.checkFields.add(CheckField.MONTH);
    return this;
  }

  public SparkTableInfoChecker withYear(List<String> partitionColumns) {
    Transform yearTransform = Expressions.years(partitionColumns.get(0));
    this.expectedTableInfo.setYear(yearTransform);
    this.checkFields.add(CheckField.YEAR);
    return this;
  }

  public SparkTableInfoChecker withTruncate(int width, List<String> partitionColumns) {
    Transform truncateTransform =
        Expressions.apply(
            "truncate", Expressions.literal(width), Expressions.column(partitionColumns.get(0)));
    this.expectedTableInfo.setTruncate(truncateTransform);
    this.checkFields.add(CheckField.TRUNCATE);
    return this;
  }

  public SparkTableInfoChecker withComment(String comment) {
    this.expectedTableInfo.setComment(comment);
    this.checkFields.add(CheckField.COMMENT);
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
                  Assertions.assertEquals(
                      expectedTableInfo.getPartitions(), realTableInfo.getPartitions());
                  break;
                case BUCKET:
                  Assertions.assertEquals(expectedTableInfo.getBucket(), realTableInfo.getBucket());
                  break;
                case HOUR:
                  Assertions.assertEquals(expectedTableInfo.getHour(), realTableInfo.getHour());
                  break;
                case DAY:
                  Assertions.assertEquals(expectedTableInfo.getDay(), realTableInfo.getDay());
                  break;
                case MONTH:
                  Assertions.assertEquals(expectedTableInfo.getMonth(), realTableInfo.getMonth());
                  break;
                case YEAR:
                  Assertions.assertEquals(expectedTableInfo.getYear(), realTableInfo.getYear());
                  break;
                case TRUNCATE:
                  Assertions.assertEquals(
                      expectedTableInfo.getTruncate(), realTableInfo.getTruncate());
                  break;
                case COMMENT:
                  Assertions.assertEquals(
                      expectedTableInfo.getComment(), realTableInfo.getComment());
                  break;
                default:
                  Assertions.fail(checkField + " not checked");
                  break;
              }
            });
  }
}
