/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.partitions;

import com.datastrato.gravitino.rel.expressions.Expression;
import java.util.Map;

/** A partition represents a result of partitioning a table. */
public interface Partition extends Expression {

  /** @return The name of the partition. */
  String name();

  /** @return The properties of the partition, such as statistics, location, etc. */
  Map<String, String> properties();

  @Override
  default Expression[] children() {
    return Expression.EMPTY_EXPRESSION;
  }
}
