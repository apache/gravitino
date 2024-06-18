/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import com.datastrato.gravitino.catalog.lakehouse.paimon.ops.PaimonTableOps;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.google.common.base.Preconditions;

/** Utilities of {@link PaimonTableOps} to support table operation. */
public class TableOpsUtils {

  public static void checkColumn(String fieldName, Expression defaultValue, boolean autoIncrement) {
    checkColumnDefaultValue(fieldName, defaultValue);
    checkColumnAutoIncrement(fieldName, autoIncrement);
  }

  private static void checkColumnDefaultValue(String fieldName, Expression defaultValue) {
    Preconditions.checkArgument(
        defaultValue.equals(Column.DEFAULT_VALUE_NOT_SET),
        String.format(
            "Paimon does not support column default value. Illegal column: %s.", fieldName));
  }

  private static void checkColumnAutoIncrement(String fieldName, boolean autoIncrement) {
    Preconditions.checkArgument(
        !autoIncrement,
        String.format(
            "Paimon does not support auto increment column. Illegal column: %s.", fieldName));
  }
}
