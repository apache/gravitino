/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.util;

import static com.datastrato.graviton.util.TransformUtils.field;
import static com.datastrato.graviton.util.TransformUtils.function;
import static com.datastrato.graviton.util.TransformUtils.literal;

import com.datastrato.graviton.meta.rel.transforms.FieldTransform;
import com.datastrato.graviton.meta.rel.transforms.LiteralTransform;
import com.datastrato.graviton.meta.rel.transforms.RefTransform;
import com.datastrato.graviton.meta.rel.transforms.Transforms;
import com.datastrato.graviton.rel.Column;
import io.substrait.expression.ImmutableExpression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTransformUtils {

  @Test
  public void testNamedReference() {
    Column column =
        new Column() {
          @Override
          public String name() {
            return "col_1";
          }

          @Override
          public Type dataType() {
            return TypeCreator.NULLABLE.I8;
          }

          @Override
          public String comment() {
            return null;
          }
        };
    Transforms.NamedReference expected =
        new Transforms.NamedReference(new String[] {column.name()});

    Assertions.assertEquals(expected, field(column));
  }

  @Test
  public void testLiteralTransform() {
    ImmutableExpression.I32Literal num = ImmutableExpression.I32Literal.builder().value(64).build();
    Transforms.LiteralReference expected = new Transforms.LiteralReference(num);

    Assertions.assertEquals(expected, literal(num));
  }

  @Test
  public void testFunctionTransform() {
    Column column =
        new Column() {
          @Override
          public String name() {
            return "col_1";
          }

          @Override
          public Type dataType() {
            return TypeCreator.NULLABLE.I8;
          }

          @Override
          public String comment() {
            return null;
          }
        };
    FieldTransform arg1 = new Transforms.NamedReference(new String[] {column.name()});
    LiteralTransform arg2 = literal(ImmutableExpression.StrLiteral.builder().value("bar").build());
    Transforms.FunctionTrans expected =
        new Transforms.FunctionTrans("foo", new RefTransform[] {arg1, arg2});

    Assertions.assertEquals(expected, function("foo", new RefTransform[] {arg1, arg2}));
  }
}
