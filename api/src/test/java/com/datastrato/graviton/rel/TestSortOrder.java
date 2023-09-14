/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.SortOrder.Direction;
import com.datastrato.graviton.rel.SortOrder.NullOrder;
import com.datastrato.graviton.rel.transforms.Transform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestSortOrder {

  @Test
  void testSortOrder() {
    SortOrder.SortOrderBuilder builder = new SortOrder.SortOrderBuilder();
    builder.withNullOrder(NullOrder.FIRST);
    builder.withDirection(Direction.ASC);
    builder.withTransform(
        new Transform() {
          @Override
          public String name() {
            return "a";
          }

          @Override
          public Transform[] arguments() {
            return new Transform[0];
          }
        });

    SortOrder sortOrder = builder.build();

    Assertions.assertEquals(NullOrder.FIRST, sortOrder.getNullOrder());
    Assertions.assertEquals(Direction.ASC, sortOrder.getDirection());
    Assertions.assertEquals("a", sortOrder.getTransform().name());

    builder.withNullOrder(NullOrder.LAST);
    builder.withDirection(Direction.DESC);
    builder.withTransform(
        new Transform() {
          @Override
          public String name() {
            return "b";
          }

          @Override
          public Transform[] arguments() {
            return new Transform[0];
          }
        });
    sortOrder = builder.build();

    Assertions.assertEquals(NullOrder.LAST, sortOrder.getNullOrder());
    Assertions.assertEquals(Direction.DESC, sortOrder.getDirection());
    Assertions.assertEquals("b", sortOrder.getTransform().name());
  }
}
