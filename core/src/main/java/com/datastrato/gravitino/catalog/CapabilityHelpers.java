/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.NAME_OF_IDENTITY;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
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

    String name = applyCapabilitiesOnName(scope, ident.name(), capabilities);
    return NameIdentifier.of(namespace, name);
  }

  public static NameIdentifier[] applyCaseSensitive(
      NameIdentifier[] idents, Capability.Scope scope, OperationDispatcher operationDispatcher) {
    return Arrays.stream(idents)
        .map(ident -> applyCaseSensitive(ident, scope, operationDispatcher))
        .toArray(NameIdentifier[]::new);
  }

  public static NameIdentifier applyCaseSensitive(
      NameIdentifier ident, Capability.Scope scope, OperationDispatcher operationDispatcher) {
    Capability capabilities = operationDispatcher.getCatalogCapability(ident);
    Namespace namespace = applyCaseSensitive(ident.namespace(), scope, operationDispatcher);

    String name = applyCaseSensitiveOnName(scope, ident.name(), capabilities);
    return NameIdentifier.of(namespace, name);
  }

  public static Namespace applyCaseSensitive(
      Namespace namespace, Capability.Scope identScope, OperationDispatcher operationDispatcher) {
    String metalake = namespace.level(0);
    String catalog = namespace.level(1);
    if (identScope == Capability.Scope.TABLE
        || identScope == Capability.Scope.FILESET
        || identScope == Capability.Scope.TOPIC) {
      String schema = namespace.level(namespace.length() - 1);
      Capability capabilities = operationDispatcher.getCatalogCapability(namespace);
      schema = applyCaseSensitiveOnName(Capability.Scope.SCHEMA, schema, capabilities);
      return Namespace.of(metalake, catalog, schema);
    }
    return namespace;
  }

  public static Transform[] applyCapabilities(Transform[] transforms, Capability capabilities) {
    return Arrays.stream(transforms)
        .map(t -> applyCapabilities(t, capabilities))
        .toArray(Transform[]::new);
  }

  public static Distribution applyCapabilities(Distribution distribution, Capability capabilities) {
    Expression[] expressions = applyCapabilities(distribution.expressions(), capabilities);
    return Distributions.of(distribution.strategy(), distribution.number(), expressions);
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
      schema = applyCapabilitiesOnName(Capability.Scope.SCHEMA, schema, capabilities);
      return Namespace.of(metalake, catalog, schema);
    }
    return namespace;
  }

  private static Index applyCapabilities(Index index, Capability capabilities) {
    return Indexes.of(
        index.type(), index.name(), applyCapabilities(index.fieldNames(), capabilities));
  }

  private static String[][] applyCapabilities(String[][] fieldNames, Capability capabilities) {
    String[][] standardizeFieldNames = new String[fieldNames.length][];
    for (int i = 0; i < standardizeFieldNames.length; i++) {
      standardizeFieldNames[i] = applyCapabilities(fieldNames[i], capabilities);
    }
    return standardizeFieldNames;
  }

  private static String[] applyCapabilities(String[] fieldName, Capability capabilities) {
    String[] sensitiveOnColumnName = applyCaseSensitiveOnColumnName(fieldName, capabilities);
    applyNameSpecification(Capability.Scope.COLUMN, sensitiveOnColumnName[0], capabilities);
    return sensitiveOnColumnName;
  }

  private static Transform applyCapabilities(Transform transform, Capability capabilities) {
    if (transform instanceof Transform.SingleFieldTransform) {
      String[] standardizeFieldName =
          applyCapabilities(((Transform.SingleFieldTransform) transform).fieldName(), capabilities);
      switch (transform.name()) {
        case NAME_OF_IDENTITY:
          return Transforms.identity(standardizeFieldName);
        case Transforms.NAME_OF_YEAR:
          return Transforms.year(standardizeFieldName);
        case Transforms.NAME_OF_MONTH:
          return Transforms.month(standardizeFieldName);
        case Transforms.NAME_OF_DAY:
          return Transforms.day(standardizeFieldName);
        case Transforms.NAME_OF_HOUR:
          return Transforms.hour(standardizeFieldName);
        default:
          throw new IllegalArgumentException("Unsupported transform: " + transform.name());
      }

    } else if (transform instanceof Transforms.BucketTransform) {
      Transforms.BucketTransform bucketTransform = (Transforms.BucketTransform) transform;
      return Transforms.bucket(
          bucketTransform.numBuckets(),
          applyCapabilities(bucketTransform.fieldNames(), capabilities));

    } else if (transform instanceof Transforms.TruncateTransform) {
      Transforms.TruncateTransform truncateTransform = (Transforms.TruncateTransform) transform;
      return Transforms.truncate(
          truncateTransform.width(),
          applyCapabilities(truncateTransform.fieldName(), capabilities));

    } else if (transform instanceof Transforms.ListTransform) {
      return Transforms.list(
          applyCapabilities(((Transforms.ListTransform) transform).fieldNames(), capabilities));

    } else if (transform instanceof Transforms.RangeTransform) {
      return Transforms.range(
          applyCapabilities(((Transforms.RangeTransform) transform).fieldName(), capabilities));

    } else if (transform instanceof Transforms.ApplyTransform) {
      return Transforms.apply(
          transform.name(), applyCapabilities(transform.arguments(), capabilities));

    } else {
      throw new IllegalArgumentException("Unsupported transform: " + transform.name());
    }
  }

  private static SortOrder applyCapabilities(SortOrder sortOrder, Capability capabilities) {
    Expression expression = applyCapabilities(sortOrder.expression(), capabilities);
    return SortOrders.of(expression, sortOrder.direction(), sortOrder.nullOrdering());
  }

  private static Expression[] applyCapabilities(Expression[] expressions, Capability capabilities) {
    return Arrays.stream(expressions)
        .map(e -> applyCapabilities(e, capabilities))
        .toArray(Expression[]::new);
  }

  private static Expression applyCapabilities(Expression expression, Capability capabilities) {
    if (expression instanceof NamedReference.FieldReference) {
      NamedReference.FieldReference ref = (NamedReference.FieldReference) expression;
      String[] fieldName = applyCapabilities(ref.fieldName(), capabilities);
      return NamedReference.field(fieldName);

    } else if (expression instanceof FunctionExpression) {
      FunctionExpression functionExpression = (FunctionExpression) expression;
      return FunctionExpression.of(
          functionExpression.functionName(),
          applyCapabilities(functionExpression.arguments(), capabilities));
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
    String[] fieldName = applyCaseSensitiveOnColumnName(change.fieldName(), capabilities);
    applyNameSpecification(Capability.Scope.COLUMN, fieldName[0], capabilities);

    if (change instanceof TableChange.AddColumn) {
      return applyCapabilities((TableChange.AddColumn) change, capabilities);

    } else if (change instanceof TableChange.UpdateColumnNullability) {
      return applyCapabilities((TableChange.UpdateColumnNullability) change, capabilities);

    } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
      return applyCapabilities(((TableChange.UpdateColumnDefaultValue) change), capabilities);

    } else if (change instanceof TableChange.RenameColumn) {
      return applyCapabilities((TableChange.RenameColumn) change, capabilities);

    } else if (change instanceof TableChange.DeleteColumn) {
      return TableChange.deleteColumn(fieldName, ((TableChange.DeleteColumn) change).getIfExists());

    } else if (change instanceof TableChange.UpdateColumnAutoIncrement) {
      return TableChange.updateColumnAutoIncrement(
          fieldName, ((TableChange.UpdateColumnAutoIncrement) change).isAutoIncrement());

    } else if (change instanceof TableChange.UpdateColumnComment) {
      return TableChange.updateColumnComment(
          fieldName, ((TableChange.UpdateColumnComment) change).getNewComment());

    } else if (change instanceof TableChange.UpdateColumnPosition) {
      TableChange.UpdateColumnPosition updateColumnPosition =
          (TableChange.UpdateColumnPosition) change;
      if (updateColumnPosition.getPosition() instanceof TableChange.After) {
        TableChange.After afterPosition = (TableChange.After) updateColumnPosition.getPosition();
        String afterFieldName =
            applyCaseSensitiveOnName(
                Capability.Scope.COLUMN, afterPosition.getColumn(), capabilities);
        applyNameSpecification(Capability.Scope.COLUMN, afterFieldName, capabilities);
        return TableChange.updateColumnPosition(
            fieldName, TableChange.ColumnPosition.after(afterFieldName));
      }
      return TableChange.updateColumnPosition(fieldName, updateColumnPosition.getPosition());

    } else if (change instanceof TableChange.UpdateColumnType) {
      return TableChange.updateColumnType(
          fieldName, ((TableChange.UpdateColumnType) change).getNewDataType());

    } else {
      throw new IllegalArgumentException("Unsupported column change: " + change);
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

    String[] standardizeFieldName =
        Arrays.copyOf(addColumn.fieldName(), addColumn.fieldName().length);
    standardizeFieldName[0] = appliedColumn.name();
    return TableChange.addColumn(
        standardizeFieldName,
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
    String[] fieldName = applyCapabilities(renameColumn.fieldName(), capabilities);
    String newName = renameColumn.getNewName();
    if (fieldName.length == 1) {
      newName = applyCapabilitiesOnName(Capability.Scope.COLUMN, newName, capabilities);
    }
    return TableChange.renameColumn(fieldName, newName);
  }

  private static Column applyCapabilities(Column column, Capability capabilities) {
    applyColumnNotNull(column, capabilities);
    applyColumnDefaultValue(column, capabilities);

    return Column.of(
        applyCapabilitiesOnName(Capability.Scope.COLUMN, column.name(), capabilities),
        column.dataType(),
        column.comment(),
        column.nullable(),
        column.autoIncrement(),
        column.defaultValue());
  }

  private static String applyCapabilitiesOnName(
      Capability.Scope scope, String name, Capability capabilities) {
    String standardizeName = applyCaseSensitiveOnName(scope, name, capabilities);
    applyNameSpecification(scope, standardizeName, capabilities);
    return standardizeName;
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
