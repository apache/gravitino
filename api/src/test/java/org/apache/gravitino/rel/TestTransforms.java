/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.rel;

import static org.apache.gravitino.rel.expressions.NamedReference.field;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.NAME_OF_DAY;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.NAME_OF_HOUR;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.NAME_OF_IDENTITY;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.NAME_OF_MONTH;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.NAME_OF_YEAR;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.apply;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.day;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.hour;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.identity;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.month;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.year;

import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTransforms {
  @Test
  public void testBucketTransform() {
    Column column = Column.of("col_1", Types.ByteType.get());
    String[] fieldName = new String[] {column.name()};

    Transform bucket = Transforms.bucket(10, fieldName);
    Expression[] arguments = bucket.arguments();
    Assertions.assertEquals(2, arguments.length);
    Assertions.assertInstanceOf(Literals.LiteralImpl.class, arguments[0]);
    Assertions.assertEquals(10, ((Literals.LiteralImpl<Integer>) arguments[0]).value());
    Assertions.assertEquals(fieldName, ((NamedReference.FieldReference) arguments[1]).fieldName());
  }

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
