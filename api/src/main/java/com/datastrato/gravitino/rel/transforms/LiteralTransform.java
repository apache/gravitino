/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.transforms;

import io.substrait.expression.Expression;

/**
 * A LiteralTransform is a transform that references a {@link
 * io.substrait.expression.Expression.Literal}.
 */
public abstract class LiteralTransform implements RefTransform<Expression.Literal> {

  private static final String LITERAL_REFERENCE = "literal_reference";

  @Override
  public String name() {
    return LITERAL_REFERENCE;
  }
}
