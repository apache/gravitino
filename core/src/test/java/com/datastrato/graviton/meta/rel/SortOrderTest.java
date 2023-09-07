/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.meta.rel;

import com.datastrato.graviton.meta.rel.transforms.Transforms;
import com.datastrato.graviton.rel.SortOrder.Direction;
import com.datastrato.graviton.rel.SortOrder.NullOrder;
import com.datastrato.graviton.rel.Transform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SortOrderTest {

  @Test
  void testSortOrder() {
    GenericSortOrder.GenericSortOrderBuilder builder =
        new GenericSortOrder.GenericSortOrderBuilder();
    builder.nullOrder(NullOrder.FIRST);
    builder.direction(Direction.ASC);
    builder.transform(Transforms.field(new String[] {"a"}));
    GenericSortOrder sortOrder = builder.build();

    Assertions.assertEquals(NullOrder.FIRST, sortOrder.nullOrder());
    Assertions.assertEquals(Direction.ASC, sortOrder.direction());
    Assertions.assertEquals(Transforms.field(new String[] {"a"}), sortOrder.transform());

    builder.nullOrder(NullOrder.LAST);
    builder.direction(Direction.DESC);
    builder.transform(
        Transforms.function("data", new Transform[] {Transforms.field(new String[] {"a"})}));
    sortOrder = builder.build();

    Assertions.assertEquals(NullOrder.LAST, sortOrder.nullOrder());
    Assertions.assertEquals(Direction.DESC, sortOrder.direction());

    Assertions.assertEquals(
        Transforms.function("data", new Transform[] {Transforms.field(new String[] {"a"})}),
        sortOrder.transform());
  }
}
