/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.types.Type;
import java.util.Map;

/**
 * An interface representing a column of a {@link Table}. It defines basic properties of a column,
 * such as name and data type.
 *
 * <p>Catalog implementation needs to implement it. They should consume it in APIs like {@link
 * TableCatalog#createTable(NameIdentifier, Column[], String, Map)}, and report it in {@link
 * Table#columns()} a default value and a generation expression.
 */
public interface Column {

  Expression DEFAULT_VALUE_NOT_SET = () -> Expression.EMPTY_EXPRESSION;

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
}
