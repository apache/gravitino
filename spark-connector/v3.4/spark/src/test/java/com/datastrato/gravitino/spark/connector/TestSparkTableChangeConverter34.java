/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.expressions.literals.Literals;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSparkTableChangeConverter34 {

  private SparkTableChangeConverter sparkTableChangeConverter =
      new SparkTableChangeConverter34(new SparkTypeConverter34());

  @Test
  void testUpdateColumnDefaultValue() {
    String[] fieldNames = new String[] {"col"};
    String defaultValue = "col_default_value";
    TableChange.UpdateColumnDefaultValue sparkUpdateColumnDefaultValue =
        (TableChange.UpdateColumnDefaultValue)
            TableChange.updateColumnDefaultValue(fieldNames, defaultValue);

    com.datastrato.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkUpdateColumnDefaultValue);

    Assertions.assertTrue(
        gravitinoChange
            instanceof com.datastrato.gravitino.rel.TableChange.UpdateColumnDefaultValue);
    com.datastrato.gravitino.rel.TableChange.UpdateColumnDefaultValue
        gravitinoUpdateColumnDefaultValue =
            (com.datastrato.gravitino.rel.TableChange.UpdateColumnDefaultValue) gravitinoChange;

    Assertions.assertArrayEquals(
        sparkUpdateColumnDefaultValue.fieldNames(), gravitinoUpdateColumnDefaultValue.fieldName());
    Assertions.assertEquals(
        Literals.stringLiteral(defaultValue),
        gravitinoUpdateColumnDefaultValue.getNewDefaultValue());
  }
}
