/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import static com.datastrato.gravitino.rel.expressions.NamedReference.field;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.NAME_OF_DAY;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.NAME_OF_HOUR;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.NAME_OF_IDENTITY;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.NAME_OF_MONTH;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.NAME_OF_YEAR;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.apply;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.day;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.hour;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.identity;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.month;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.year;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTransforms {
  @Test
  public void testSingleFieldTransform() {
    Column column = Column.of("col_1", Types.ByteType.get());
    String[] fieldName = new String[] {column.name()};

    Transform.SingleFieldTransform identity = identity(fieldName);
    Assertions.assertEquals(NAME_OF_IDENTITY, identity.name());
    Assertions.assertEquals(fieldName, identity.fieldName());

    Transform.SingleFieldTransform year = year(fieldName);
    Assertions.assertEquals(NAME_OF_YEAR, year.name());
    Assertions.assertEquals(fieldName, year.fieldName());

    Transform.SingleFieldTransform month = month(fieldName);
    Assertions.assertEquals(NAME_OF_MONTH, month.name());
    Assertions.assertEquals(fieldName, month.fieldName());

    Transform.SingleFieldTransform day = day(fieldName);
    Assertions.assertEquals(NAME_OF_DAY, day.name());
    Assertions.assertEquals(fieldName, day.fieldName());

    Transform.SingleFieldTransform hour = hour(fieldName);
    Assertions.assertEquals(NAME_OF_HOUR, hour.name());
    Assertions.assertEquals(fieldName, hour.fieldName());
  }

  @Test
  public void testApplyTransform() {
    Column column = Column.of("col_1", Types.ByteType.get());
    // partition by foo(col_1, 'bar')
    NamedReference.FieldReference arg1 = field(column.name());
    Literals.LiteralImpl<String> arg2 = Literals.stringLiteral("bar");
    Transform applyTransform = apply("foo", new Expression[] {arg1, arg2});
    Assertions.assertEquals("foo", applyTransform.name());
    Assertions.assertEquals(2, applyTransform.arguments().length);
    Assertions.assertEquals(arg1, applyTransform.arguments()[0]);
    Assertions.assertEquals(arg2, applyTransform.arguments()[1]);
  }
}
