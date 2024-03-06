/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.util.spark;

import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo.SparkColumnInfo;
import java.util.ArrayList;
import java.util.List;
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
