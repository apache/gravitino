/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.rel;

import static com.datastrato.gravitino.rel.expressions.NamedReference.field;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestSortOrder {

  @Test
  void testSortOrder() {
    NamedReference.FieldReference fieldReference = field("field1");
    SortOrder sortOrder =
        SortOrders.of(fieldReference, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);

    Assertions.assertEquals(NullOrdering.NULLS_FIRST, sortOrder.nullOrdering());
    Assertions.assertEquals(SortDirection.ASCENDING, sortOrder.direction());
    Assertions.assertTrue(sortOrder.expression() instanceof NamedReference);
    Assertions.assertArrayEquals(
        new String[] {"field1"}, ((NamedReference) sortOrder.expression()).fieldName());

    Expression date = FunctionExpression.of("date", new Expression[] {field("b")});
    sortOrder = SortOrders.of(date, SortDirection.DESCENDING, NullOrdering.NULLS_LAST);
    Assertions.assertEquals(NullOrdering.NULLS_LAST, sortOrder.nullOrdering());
    Assertions.assertEquals(SortDirection.DESCENDING, sortOrder.direction());

    Assertions.assertTrue(sortOrder.expression() instanceof FunctionExpression);
    Assertions.assertEquals("date", ((FunctionExpression) sortOrder.expression()).functionName());
    Assertions.assertArrayEquals(
        new String[] {"b"},
        ((FunctionExpression) sortOrder.expression()).arguments()[0].references()[0].fieldName());
  }
}
