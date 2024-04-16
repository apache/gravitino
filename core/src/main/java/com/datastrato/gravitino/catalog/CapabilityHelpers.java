/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
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
              if (change instanceof TableChange.ColumnChange) {
                return applyCapabilities((TableChange.ColumnChange) change, capabilities);

              } else if (change instanceof TableChange.RenameTable) {
                return applyCapabilities((TableChange.RenameTable) change, capabilities);
              }
              return change;
            })
        .toArray(TableChange[]::new);
  }

  public static FilesetChange[] applyCapabilities(
      Capability capabilities, FilesetChange... changes) {
    return Arrays.stream(changes)
        .map(
            change -> {
              if (change instanceof FilesetChange.RenameFileset) {
                return applyCapabilities((FilesetChange.RenameFileset) change, capabilities);
              }
              return change;
            })
        .toArray(FilesetChange[]::new);
  }

  public static NameIdentifier[] applyCapabilities(
      NameIdentifier[] idents, Capability.Scope scope, Capability capabilities) {
    return Arrays.stream(idents)
        .map(ident -> applyCapabilities(ident, scope, capabilities))
        .toArray(NameIdentifier[]::new);
  }

  public static NameIdentifier applyCapabilities(
      NameIdentifier ident, Capability.Scope scope, Capability capabilities) {
    Namespace namespace = ident.namespace();
    namespace = applyCapabilities(namespace, scope, capabilities);

    String name = applyCaseSensitiveOnName(scope, ident.name(), capabilities);
    applyNameSpecification(scope, name, capabilities);
    return NameIdentifier.of(namespace, name);
  }

  public static Transform[] applyCapabilities(Transform[] transforms, Capability capabilities) {
    return Arrays.stream(transforms)
        .map(t -> applyCapabilities(t, capabilities))
        .toArray(Transform[]::new);
  }

  public static Distribution applyCapabilities(Distribution distribution, Capability capabilities) {
    applyCapabilities(distribution.expressions(), capabilities);
    return distribution;
  }

  public static SortOrder[] applyCapabilities(SortOrder[] sortOrders, Capability capabilities) {
    return Arrays.stream(sortOrders)
        .map(s -> applyCapabilities(s, capabilities))
        .toArray(SortOrder[]::new);
  }

  public static Index[] applyCapabilities(Index[] indexes, Capability capabilities) {
    return Arrays.stream(indexes)
        .map(i -> applyCapabilities(i, capabilities))
        .toArray(Index[]::new);
  }

  public static Namespace applyCapabilities(
      Namespace namespace, Capability.Scope identScope, Capability capabilities) {
    String metalake = namespace.level(0);
    String catalog = namespace.level(1);
    if (identScope == Capability.Scope.TABLE
        || identScope == Capability.Scope.FILESET
        || identScope == Capability.Scope.TOPIC) {
      String schema = namespace.level(namespace.length() - 1);
      schema = applyCaseSensitiveOnName(Capability.Scope.SCHEMA, schema, capabilities);
      applyNameSpecification(Capability.Scope.SCHEMA, schema, capabilities);
      return Namespace.of(metalake, catalog, schema);
    }
    return namespace;
  }

  private static Index applyCapabilities(Index index, Capability capabilities) {
    String[][] fieldNames = index.fieldNames();
    for (String[] fieldName : fieldNames) {
      String[] sensitiveOnColumnName = applyCaseSensitiveOnColumnName(fieldName, capabilities);
      fieldName[0] = sensitiveOnColumnName[0];
      applyNameSpecification(Capability.Scope.COLUMN, sensitiveOnColumnName[0], capabilities);
    }
    return index;
  }

  private static Transform applyCapabilities(Transform transform, Capability capabilities) {
    for (Expression arg : transform.arguments()) {
      applyCapabilities(arg, capabilities);
    }
    return transform;
  }

  private static SortOrder applyCapabilities(SortOrder sortOrder, Capability capabilities) {
    applyCapabilities(sortOrder.expression(), capabilities);
    return sortOrder;
  }

  private static Expression[] applyCapabilities(Expression[] expressions, Capability capabilities) {
    return Arrays.stream(expressions)
        .map(e -> applyCapabilities(e, capabilities))
        .toArray(Expression[]::new);
  }

  private static Expression applyCapabilities(Expression expression, Capability capabilities) {
    if (expression instanceof NamedReference.FieldReference) {
      NamedReference.FieldReference ref = (NamedReference.FieldReference) expression;
      String[] fieldName = applyCaseSensitiveOnColumnName(ref.fieldName(), capabilities);
      ref.fieldName()[0] = fieldName[0];
      applyNameSpecification(Capability.Scope.COLUMN, fieldName[0], capabilities);
    }
    for (Expression child : expression.children()) {
      applyCapabilities(child, capabilities);
    }
    return expression;
  }

  private static FilesetChange applyCapabilities(
      FilesetChange.RenameFileset renameFileset, Capability capabilities) {
    String newName =
        applyCaseSensitiveOnName(
            Capability.Scope.FILESET, renameFileset.getNewName(), capabilities);
    applyNameSpecification(Capability.Scope.FILESET, newName, capabilities);
    return FilesetChange.rename(newName);
  }

  private static TableChange applyCapabilities(
      TableChange.RenameTable renameTable, Capability capabilities) {
    String newName =
        applyCaseSensitiveOnName(Capability.Scope.TABLE, renameTable.getNewName(), capabilities);
    applyNameSpecification(Capability.Scope.TABLE, newName, capabilities);
    return TableChange.rename(newName);
  }

  private static TableChange applyCapabilities(
      TableChange.ColumnChange change, Capability capabilities) {
    if (change instanceof TableChange.AddColumn) {
      return applyCapabilities((TableChange.AddColumn) change, capabilities);

    } else if (change instanceof TableChange.UpdateColumnNullability) {
      return applyCapabilities((TableChange.UpdateColumnNullability) change, capabilities);

    } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
      return applyCapabilities(((TableChange.UpdateColumnDefaultValue) change), capabilities);

    } else if (change instanceof TableChange.RenameColumn) {
      return applyCapabilities((TableChange.RenameColumn) change, capabilities);

    } else {
      String[] fieldName = applyCaseSensitiveOnColumnName(change.fieldName(), capabilities);
      change.fieldName()[0] = fieldName[0];
      applyNameSpecification(Capability.Scope.COLUMN, fieldName[0], capabilities);
      return change;
    }
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

  private static TableChange applyCapabilities(
      TableChange.RenameColumn renameColumn, Capability capabilities) {
    String[] fieldName = applyCaseSensitiveOnColumnName(renameColumn.fieldName(), capabilities);
    String newName = renameColumn.getNewName();
    if (fieldName.length == 1) {
      applyNameSpecification(Capability.Scope.COLUMN, fieldName[0], capabilities);
      newName =
          applyCaseSensitiveOnName(
              Capability.Scope.COLUMN, renameColumn.getNewName(), capabilities);
      applyNameSpecification(Capability.Scope.COLUMN, newName, capabilities);
    }
    return TableChange.renameColumn(fieldName, newName);
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
