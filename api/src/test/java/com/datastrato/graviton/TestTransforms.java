/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_DAY;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_HOUR;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_MONTH;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_YEAR;
import static com.datastrato.graviton.rel.transforms.Transforms.day;
import static com.datastrato.graviton.rel.transforms.Transforms.field;
import static com.datastrato.graviton.rel.transforms.Transforms.function;
import static com.datastrato.graviton.rel.transforms.Transforms.hour;
import static com.datastrato.graviton.rel.transforms.Transforms.identity;
import static com.datastrato.graviton.rel.transforms.Transforms.literal;
import static com.datastrato.graviton.rel.transforms.Transforms.month;
import static com.datastrato.graviton.rel.transforms.Transforms.year;

import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.transforms.FieldTransform;
import com.datastrato.graviton.rel.transforms.LiteralTransform;
import com.datastrato.graviton.rel.transforms.RefTransform;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import io.substrait.expression.ImmutableExpression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTransforms {
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
    String[] fieldName = new String[] {column.name()};
    Transforms.NamedReference expected = new Transforms.NamedReference(fieldName);

    Assertions.assertEquals(expected, field(column));

    FieldTransform identity = identity(fieldName);
    Assertions.assertEquals(fieldName, identity.value());

    Transform year = year(fieldName);
    Assertions.assertEquals(NAME_OF_YEAR, year.name());
    Assertions.assertEquals(fieldName, ((FieldTransform) year.arguments()[0]).value());

    Transform month = month(fieldName);
    Assertions.assertEquals(NAME_OF_MONTH, month.name());
    Assertions.assertEquals(fieldName, ((FieldTransform) month.arguments()[0]).value());

    Transform day = day(fieldName);
    Assertions.assertEquals(NAME_OF_DAY, day.name());
    Assertions.assertEquals(fieldName, ((FieldTransform) day.arguments()[0]).value());

    Transform hour = hour(fieldName);
    Assertions.assertEquals(NAME_OF_HOUR, hour.name());
    Assertions.assertEquals(fieldName, ((FieldTransform) hour.arguments()[0]).value());
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
