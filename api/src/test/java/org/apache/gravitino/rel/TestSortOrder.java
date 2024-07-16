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

import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
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
