/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.SortOrder.Direction;
import com.datastrato.graviton.rel.SortOrder.NullOrdering;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.datastrato.graviton.rel.transforms.Transforms.FunctionTrans;
import com.datastrato.graviton.rel.transforms.Transforms.NamedReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestSortOrder {

  @Test
  void testSortOrder() {
    SortOrder.SortOrderBuilder builder = new SortOrder.SortOrderBuilder();
    builder.withNullOrder(NullOrdering.FIRST);
    builder.withDirection(Direction.ASC);

    Transform transform = Transforms.field(new String[] {"field1"});
    builder.withTransform(transform);
    SortOrder sortOrder = builder.build();

    Assertions.assertEquals(NullOrdering.FIRST, sortOrder.getNullOrder());
    Assertions.assertEquals(Direction.ASC, sortOrder.getDirection());
    Assertions.assertTrue(sortOrder.getTransform() instanceof NamedReference);
    Assertions.assertArrayEquals(
        new String[] {"field1"}, ((NamedReference) sortOrder.getTransform()).value());

    builder.withNullOrder(NullOrdering.LAST);
    builder.withDirection(Direction.DESC);
    transform = Transforms.function("date", new Transform[] {Transforms.field(new String[] {"b"})});
    builder.withTransform(transform);
    sortOrder = builder.build();
    Assertions.assertEquals(NullOrdering.LAST, sortOrder.getNullOrder());
    Assertions.assertEquals(Direction.DESC, sortOrder.getDirection());

    Assertions.assertTrue(sortOrder.getTransform() instanceof FunctionTrans);
    Assertions.assertEquals("date", ((FunctionTrans) sortOrder.getTransform()).name());
    Assertions.assertArrayEquals(
        new String[] {"b"}, ((NamedReference) sortOrder.getTransform().arguments()[0]).value());
  }
}
