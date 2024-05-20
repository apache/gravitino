/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.types.Type;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Map;
import java.util.Objects;

/**
 * An interface representing a column of a {@link Table}. It defines basic properties of a column,
 * such as name and data type.
 *
 * <p>Catalog implementation needs to implement it. They should consume it in APIs like {@link
 * TableCatalog#createTable(NameIdentifier, Column[], String, Map)}, and report it in {@link
 * Table#columns()} a default value and a generation expression.
 */
@Evolving
public interface Column {

  /**
   * A default value that indicates the default value is not set. This is used in {@link
   * #defaultValue()}.
   */
  Expression DEFAULT_VALUE_NOT_SET = () -> Expression.EMPTY_EXPRESSION;

  /**
   * A default value that indicates the default value will be set to the current timestamp. This is
   * used in {@link #defaultValue()}.
   */
  Expression DEFAULT_VALUE_OF_CURRENT_TIMESTAMP = FunctionExpression.of("current_timestamp");

  /** @return The name of this column. */
  String name();

  /** @return The data type of this column. */
  Type dataType();

  /** @return The comment of this column, null if not specified. */
  String comment();

  /** @return True if this column may produce null values. Default is true. */
  boolean nullable();

  /** @return True if this column is an auto-increment column. Default is false. */
  boolean autoIncrement();

  /**
   * @return The default value of this column, {@link Column#DEFAULT_VALUE_NOT_SET} if not specified
   */
  Expression defaultValue();

  /**
   * Create a {@link Column} instance.
   *
   * @param name The name of the column.
   * @param dataType The data type of the column.
   * @param comment The comment of the column.
   * @param defaultValue The default value of the column. {@link Column#DEFAULT_VALUE_NOT_SET} if
   *     null.
   * @return A {@link Column} instance.
   */
  static ColumnImpl of(String name, Type dataType, String comment, Expression defaultValue) {
    return of(name, dataType, comment, true, false, defaultValue);
  }

  /**
   * Create a {@link Column} instance.
   *
   * @param name The name of the column.
   * @param dataType The data type of the column.
   * @param comment The comment of the column.
   * @return A {@link Column} instance.
   */
  static ColumnImpl of(String name, Type dataType, String comment) {
    return of(name, dataType, comment, true, false, DEFAULT_VALUE_NOT_SET);
  }

  /**
   * Create a {@link Column} instance.
   *
   * @param name The name of the column.
   * @param dataType The data type of the column.
   * @return A {@link Column} instance.
   */
  static ColumnImpl of(String name, Type dataType) {
    return of(name, dataType, null, true, false, DEFAULT_VALUE_NOT_SET);
  }

  /**
   * Create a {@link Column} instance.
   *
   * @param name The name of the column.
   * @param dataType The data type of the column.
   * @param comment The comment of the column.
   * @param nullable True if the column may produce null values.
   * @param autoIncrement True if the column is an auto-increment column.
   * @param defaultValue The default value of the column. {@link Column#DEFAULT_VALUE_NOT_SET} if
   *     null.
   * @return A {@link Column} instance.
   */
  static ColumnImpl of(
      String name,
      Type dataType,
      String comment,
      boolean nullable,
      boolean autoIncrement,
      Expression defaultValue) {
    return new ColumnImpl(
        name,
        dataType,
        comment,
        nullable,
        autoIncrement,
        defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue);
  }

  /** The implementation of {@link Column} for users to use API. */
  class ColumnImpl implements Column {
    private String name;
    private Type dataType;
    private String comment;
    private boolean nullable;
    private boolean autoIncrement;
    private Expression defaultValue;

    private ColumnImpl(
        String name,
        Type dataType,
        String comment,
        boolean nullable,
        boolean autoIncrement,
        Expression defaultValue) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Column name cannot be null");
      Preconditions.checkArgument(dataType != null, "Column data type cannot be null");
      this.name = name;
      this.dataType = dataType;
      this.comment = comment;
      this.nullable = nullable;
      this.autoIncrement = autoIncrement;
      this.defaultValue = defaultValue;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Type dataType() {
      return dataType;
    }

    @Override
    public String comment() {
      return comment;
    }

    @Override
    public boolean nullable() {
      return nullable;
    }

    @Override
    public boolean autoIncrement() {
      return autoIncrement;
    }

    @Override
    public Expression defaultValue() {
      return defaultValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ColumnImpl)) {
        return false;
      }
      ColumnImpl column = (ColumnImpl) o;
      return nullable == column.nullable
          && autoIncrement == column.autoIncrement
          && Objects.equals(name, column.name)
          && Objects.equals(dataType, column.dataType)
          && Objects.equals(comment, column.comment)
          && Objects.equals(defaultValue, column.defaultValue);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, dataType, comment, nullable, autoIncrement, defaultValue);
    }
  }
}
