/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.SortOrder.Builder;
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
    Builder builder = new Builder();
    builder.withNullOrdering(NullOrdering.FIRST);
    builder.withDirection(Direction.ASC);

    Transform transform = Transforms.field(new String[] {"field1"});
    builder.withTransform(transform);
    SortOrder sortOrder = builder.build();

    Assertions.assertEquals(NullOrdering.FIRST, sortOrder.getNullOrdering());
    Assertions.assertEquals(Direction.ASC, sortOrder.getDirection());
    Assertions.assertTrue(sortOrder.getTransform() instanceof NamedReference);
    Assertions.assertArrayEquals(
        new String[] {"field1"}, ((NamedReference) sortOrder.getTransform()).value());

    builder.withNullOrdering(NullOrdering.LAST);
    builder.withDirection(Direction.DESC);
    transform = Transforms.function("date", new Transform[] {Transforms.field(new String[] {"b"})});
    builder.withTransform(transform);
    sortOrder = builder.build();
    Assertions.assertEquals(NullOrdering.LAST, sortOrder.getNullOrdering());
    Assertions.assertEquals(Direction.DESC, sortOrder.getDirection());

    Assertions.assertTrue(sortOrder.getTransform() instanceof FunctionTrans);
    Assertions.assertEquals("date", ((FunctionTrans) sortOrder.getTransform()).name());
    Assertions.assertArrayEquals(
        new String[] {"b"}, ((NamedReference) sortOrder.getTransform().arguments()[0]).value());
  }

  @Test
  void testUtils() throws Exception {
    SortOrder sortOrder =
        SortOrder.fieldSortOrder(new String[] {"a"}, Direction.ASC, NullOrdering.FIRST);
    Assertions.assertEquals(NullOrdering.FIRST, sortOrder.getNullOrdering());
    Assertions.assertEquals(Direction.ASC, sortOrder.getDirection());
    Assertions.assertTrue(sortOrder.getTransform() instanceof NamedReference);
    Assertions.assertArrayEquals(
        new String[] {"a"}, ((NamedReference) sortOrder.getTransform()).value());

    sortOrder =
        SortOrder.functionSortOrder("date", new String[] {"b"}, Direction.DESC, NullOrdering.LAST);
    Assertions.assertEquals(NullOrdering.LAST, sortOrder.getNullOrdering());
    Assertions.assertEquals(Direction.DESC, sortOrder.getDirection());
    Assertions.assertTrue(sortOrder.getTransform() instanceof FunctionTrans);
    Assertions.assertEquals("date", ((FunctionTrans) sortOrder.getTransform()).name());
    Assertions.assertArrayEquals(
        new String[] {"b"}, ((NamedReference) sortOrder.getTransform().arguments()[0]).value());
  }
}
