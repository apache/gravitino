/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta.rel.transforms;

import com.datastrato.graviton.rel.Transform;
import com.google.common.annotations.VisibleForTesting;
import io.substrait.expression.Expression;
import lombok.EqualsAndHashCode;

public class Transforms {
  @VisibleForTesting
  @EqualsAndHashCode(callSuper = false)
  public static final class NamedReference extends FieldTransform {
    private final String[] fieldName;

    public NamedReference(String[] fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String[] value() {
      return fieldName;
    }
  }

  @VisibleForTesting
  @EqualsAndHashCode(callSuper = false)
  public static final class LiteralReference extends LiteralTransform {
    private final Expression.Literal value;

    public LiteralReference(Expression.Literal value) {
      this.value = value;
    }

    @Override
    public Expression.Literal value() {
      return value;
    }
  }

  @VisibleForTesting
  @EqualsAndHashCode(callSuper = false)
  public static final class FunctionTrans extends FunctionTransform {
    private final String name;
    private final Transform[] args;

    public FunctionTrans(String name, Transform[] args) {
      this.name = name;
      this.args = args;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Transform[] arguments() {
      return args;
    }
  }
}
