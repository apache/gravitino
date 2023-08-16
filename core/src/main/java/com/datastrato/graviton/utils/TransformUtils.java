/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.util;

import com.datastrato.graviton.meta.rel.transforms.FieldTransform;
import com.datastrato.graviton.meta.rel.transforms.FunctionTransform;
import com.datastrato.graviton.meta.rel.transforms.LiteralTransform;
import com.datastrato.graviton.meta.rel.transforms.Transforms;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Transform;
import io.substrait.expression.Expression;

/** Helper methods to create logical transforms to pass into Graviton. */
public class TransformUtils {

  public static LiteralTransform literal(Expression.Literal value) {
    return new Transforms.LiteralReference(value);
  }

  public static FieldTransform field(String[] fieldName) {
    return new Transforms.NamedReference(fieldName);
  }

  public static FieldTransform field(Column column) {
    return field(new String[] {column.name()});
  }

  public static FunctionTransform function(String name, Transform[] args) {
    return new Transforms.FunctionTrans(name, args);
  }
}
