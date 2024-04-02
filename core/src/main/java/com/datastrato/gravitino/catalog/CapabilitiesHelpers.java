/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.connector.capability.HasCapabilities;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.google.common.base.Preconditions;
import java.util.Arrays;

public class CapabilitiesHelpers {

  public static Column[] applyCapabilities(Column[] columns, HasCapabilities capabilities) {
    return Arrays.stream(columns)
        .map(c -> applyCapabilities(c, capabilities))
        .toArray(Column[]::new);
  }

  public static TableChange[] applyCapabilities(
      HasCapabilities capabilities, TableChange... changes) {
    return Arrays.stream(changes)
        .map(
            change -> {
              if (change instanceof TableChange.ColumnChange) {
                if (change instanceof TableChange.AddColumn) {
                  TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
                  Column appliedColumn =
                      Column.of(
                          addColumn.fieldName()[0],
                          addColumn.getDataType(),
                          addColumn.getComment(),
                          addColumn.isNullable(),
                          addColumn.isAutoIncrement(),
                          DEFAULT_VALUE_NOT_SET);
                  String[] appliedFieldName = addColumn.fieldName();
                  appliedFieldName[0] = appliedColumn.name();
                  return TableChange.addColumn(
                      appliedFieldName,
                      appliedColumn.dataType(),
                      appliedColumn.comment(),
                      addColumn.getPosition(),
                      appliedColumn.nullable(),
                      appliedColumn.autoIncrement());
                } else if (change instanceof TableChange.UpdateColumnNullability) {
                  applyColumnNotNull(
                      ((TableChange.UpdateColumnNullability) change).nullable(), capabilities);
                }
              }
              return change;
            })
        .toArray(TableChange[]::new);
  }

  private static Column applyCapabilities(Column column, HasCapabilities capabilities) {
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
      Capability.Scope scope, String name, HasCapabilities capabilities) {
    return capabilities.caseSensitiveOnName(scope).supported() ? name : name.toLowerCase();
  }

  private static void applyColumnNotNull(Column column, HasCapabilities capabilities) {
    applyColumnNotNull(column.nullable(), capabilities);
  }

  private static void applyColumnNotNull(boolean nullable, HasCapabilities capabilities) {
    Preconditions.checkArgument(
        capabilities.columnNotNull().supported() || nullable,
        capabilities.columnNotNull().unsupportedMessage() + " Illegal column: ");
  }

  private static void applyColumnDefaultValue(Column column, HasCapabilities capabilities) {
    Preconditions.checkArgument(
        capabilities.columnDefaultValue().supported()
            || DEFAULT_VALUE_NOT_SET.equals(column.defaultValue()),
        capabilities.columnDefaultValue().unsupportedMessage()
            + " Illegal column: "
            + column.name());
  }

  private static void applyNameSpecification(
      Capability.Scope scope, String name, HasCapabilities capabilities) {
    Preconditions.checkArgument(
        capabilities.specificationOnName(scope, name).supported(),
        capabilities.specificationOnName(scope, name).unsupportedMessage()
            + " Illegal name: "
            + name);
  }
}
