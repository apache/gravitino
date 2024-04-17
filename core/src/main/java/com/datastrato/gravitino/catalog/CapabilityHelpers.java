/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.google.common.base.Preconditions;
import java.util.Arrays;

public class CapabilityHelpers {

  public static Column[] applyCapabilities(Column[] columns, Capability capabilities) {
    return Arrays.stream(columns)
        .map(c -> applyCapabilities(c, capabilities))
        .toArray(Column[]::new);
  }

  public static TableChange[] applyCapabilities(Capability capabilities, TableChange... changes) {
    return Arrays.stream(changes)
        .map(
            change -> {
              if (change instanceof TableChange.AddColumn) {
                return applyCapabilities((TableChange.AddColumn) change, capabilities);

              } else if (change instanceof TableChange.UpdateColumnNullability) {
                return applyCapabilities(
                    (TableChange.UpdateColumnNullability) change, capabilities);

              } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
                return applyCapabilities(
                    ((TableChange.UpdateColumnDefaultValue) change), capabilities);
              }
              return change;
            })
        .toArray(TableChange[]::new);
  }

  private static TableChange applyCapabilities(
      TableChange.AddColumn addColumn, Capability capabilities) {
    Column appliedColumn =
        applyCapabilities(
            Column.of(
                addColumn.fieldName()[0],
                addColumn.getDataType(),
                addColumn.getComment(),
                addColumn.isNullable(),
                addColumn.isAutoIncrement(),
                addColumn.getDefaultValue()),
            capabilities);

    return TableChange.addColumn(
        applyCaseSensitiveOnColumnName(addColumn.fieldName(), capabilities),
        appliedColumn.dataType(),
        appliedColumn.comment(),
        addColumn.getPosition(),
        appliedColumn.nullable(),
        appliedColumn.autoIncrement(),
        appliedColumn.defaultValue());
  }

  private static TableChange applyCapabilities(
      TableChange.UpdateColumnNullability updateColumnNullability, Capability capabilities) {

    applyColumnNotNull(
        String.join(".", updateColumnNullability.fieldName()),
        updateColumnNullability.nullable(),
        capabilities);

    return TableChange.updateColumnNullability(
        applyCaseSensitiveOnColumnName(updateColumnNullability.fieldName(), capabilities),
        updateColumnNullability.nullable());
  }

  private static TableChange applyCapabilities(
      TableChange.UpdateColumnDefaultValue updateColumnDefaultValue, Capability capabilities) {
    applyColumnDefaultValue(
        String.join(".", updateColumnDefaultValue.fieldName()),
        updateColumnDefaultValue.getNewDefaultValue(),
        capabilities);

    return TableChange.updateColumnDefaultValue(
        applyCaseSensitiveOnColumnName(updateColumnDefaultValue.fieldName(), capabilities),
        updateColumnDefaultValue.getNewDefaultValue());
  }

  private static Column applyCapabilities(Column column, Capability capabilities) {
    applyColumnNotNull(column, capabilities);
    applyColumnDefaultValue(column, capabilities);
    applyNameSpecification(Capability.Scope.COLUMN, column.name(), capabilities);

    return Column.of(
        applyCaseSensitiveOnName(Capability.Scope.COLUMN, column.name(), capabilities),
        column.dataType(),
        column.comment(),
        column.nullable(),
        column.autoIncrement(),
        column.defaultValue());
  }

  private static String applyCaseSensitiveOnName(
      Capability.Scope scope, String name, Capability capabilities) {
    return capabilities.caseSensitiveOnName(scope).supported() ? name : name.toLowerCase();
  }

  private static String[] applyCaseSensitiveOnColumnName(String[] name, Capability capabilities) {
    if (!capabilities.caseSensitiveOnName(Capability.Scope.COLUMN).supported()) {
      String[] standardizeColumnName = Arrays.copyOf(name, name.length);
      standardizeColumnName[0] = name[0].toLowerCase();
      return standardizeColumnName;
    }
    return name;
  }

  private static void applyColumnNotNull(Column column, Capability capabilities) {
    applyColumnNotNull(column.name(), column.nullable(), capabilities);
  }

  private static void applyColumnNotNull(
      String columnName, boolean nullable, Capability capabilities) {
    Preconditions.checkArgument(
        capabilities.columnNotNull().supported() || nullable,
        capabilities.columnNotNull().unsupportedMessage() + " Illegal column: " + columnName);
  }

  private static void applyColumnDefaultValue(Column column, Capability capabilities) {
    applyColumnDefaultValue(column.name(), column.defaultValue(), capabilities);
  }

  private static void applyColumnDefaultValue(
      String columnName, Expression defaultValue, Capability capabilities) {
    Preconditions.checkArgument(
        capabilities.columnDefaultValue().supported() || DEFAULT_VALUE_NOT_SET.equals(defaultValue),
        capabilities.columnDefaultValue().unsupportedMessage() + " Illegal column: " + columnName);
  }

  private static void applyNameSpecification(
      Capability.Scope scope, String name, Capability capabilities) {
    Preconditions.checkArgument(
        capabilities.specificationOnName(scope, name).supported(),
        capabilities.specificationOnName(scope, name).unsupportedMessage()
            + " Illegal name: "
            + name);
  }
}
