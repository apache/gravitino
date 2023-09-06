/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.meta.rel.transforms.Transforms;
import com.datastrato.graviton.rel.SortOrder.Direction;
import com.datastrato.graviton.rel.SortOrder.NullOrder;
import com.datastrato.graviton.rel.Transform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HiveSortOrderTest {

  @Test
  void testSortOrder() {
    HiveSortOrder.Builder builder = new HiveSortOrder.Builder();
    builder.withNullOrder(NullOrder.FIRST);
    builder.withDirection(Direction.ASC);
    builder.withTransform(Transforms.field(new String[] {"a"}));
    HiveSortOrder sortOrder = builder.build();

    Assertions.assertEquals(NullOrder.FIRST, sortOrder.nullOrder());
    Assertions.assertEquals(Direction.ASC, sortOrder.direction());
    Assertions.assertEquals(Transforms.field(new String[] {"a"}), sortOrder.transform());

    builder.withNullOrder(NullOrder.LAST);
    builder.withDirection(Direction.DESC);
    builder.withTransform(
        Transforms.function("data", new Transform[] {Transforms.field(new String[] {"a"})}));
    sortOrder = builder.build();

    Assertions.assertEquals(NullOrder.LAST, sortOrder.nullOrder());
    Assertions.assertEquals(Direction.DESC, sortOrder.direction());

    Assertions.assertEquals(
        Transforms.function("data", new Transform[] {Transforms.field(new String[] {"a"})}),
        sortOrder.transform());
  }
}
