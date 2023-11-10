/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.sorts;

import com.datastrato.gravitino.rel.expressions.Expression;

public interface SortOrder extends Expression {
  /** Returns the sort expression. */
  Expression expression();

  /** Returns the sort direction. */
  SortDirection direction();

  /** Returns the null ordering. */
  NullOrdering nullOrdering();

  @Override
  default Expression[] children() {
    return new Expression[] {expression()};
  }
}
