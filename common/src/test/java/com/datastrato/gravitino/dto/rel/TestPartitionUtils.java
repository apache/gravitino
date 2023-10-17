/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.toPartitions;
import static com.datastrato.gravitino.dto.rel.PartitionUtils.toTransforms;
import static com.datastrato.gravitino.rel.transforms.Transforms.field;
import static com.datastrato.gravitino.rel.transforms.Transforms.function;
import static com.datastrato.gravitino.rel.transforms.Transforms.identity;
import static com.datastrato.gravitino.rel.transforms.Transforms.literal;
import static com.datastrato.gravitino.rel.transforms.Transforms.month;
import static com.datastrato.gravitino.rel.transforms.Transforms.year;
import static io.substrait.expression.ExpressionCreator.string;

import com.datastrato.gravitino.rel.transforms.FunctionTransform;
import com.datastrato.gravitino.rel.transforms.Transform;
import com.datastrato.gravitino.rel.transforms.Transforms;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionUtils {

  @Test
  void testPartitionTransformConvert() {
    String[] field1 = new String[] {"dt"};
    String[] field2 = new String[] {"city"};
    String[] field3 = new String[] {"ts"};

    Transform identity = identity(field1);
    Transform year = year(field1);
    Transform month = month(field1);
    Transform day = Transforms.day(field1);
    Transform hour = Transforms.hour(field1);
    Transform list = Transforms.list(new String[][] {field1, field2});
    Transform range = Transforms.range(field1);
    // toYYYYMM(toDate(ts, ‘Asia/Shanghai’))
    Transform field = field(field3);
    Transform literal = literal(string(false, "Asia/Shanghai"));
    FunctionTransform toDate = function("toDate", new Transform[] {field, literal});
    FunctionTransform toYYYYMM = function("toYYYYMM", new Transform[] {toDate});

    Transform[] expectedTransforms =
        new Transform[] {identity, year, month, day, hour, list, range, toYYYYMM};
    Transform[] actualTransforms = toTransforms(toPartitions(expectedTransforms));
    Assertions.assertArrayEquals(expectedTransforms, actualTransforms);
  }
}
