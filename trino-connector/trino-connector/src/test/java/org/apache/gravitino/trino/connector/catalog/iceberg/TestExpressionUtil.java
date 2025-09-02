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
package org.apache.gravitino.trino.connector.catalog.iceberg;

import io.trino.spi.TrinoException;
import java.util.List;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExpressionUtil {

  @Test
  void testPartitionFiledToExpression() {
    List<String> partitionField = List.of("f1");
    Transform[] transforms = ExpressionUtil.partitionFiledToExpression(partitionField);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(Transforms.identity(new String[] {"f1"}), transforms[0]);

    partitionField = List.of("year(f1)");
    transforms = ExpressionUtil.partitionFiledToExpression(partitionField);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(Transforms.year("f1"), transforms[0]);

    partitionField = List.of("MONTH(f2)");
    transforms = ExpressionUtil.partitionFiledToExpression(partitionField);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(Transforms.month("f2"), transforms[0]);

    partitionField = List.of("day(f3)");
    transforms = ExpressionUtil.partitionFiledToExpression(partitionField);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(Transforms.day("f3"), transforms[0]);

    partitionField = List.of("hour(f4)");
    transforms = ExpressionUtil.partitionFiledToExpression(partitionField);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(transforms[0], Transforms.day("f4"));

    partitionField = List.of("bucket(f2,10)");
    transforms = ExpressionUtil.partitionFiledToExpression(partitionField);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(transforms[0], Transforms.bucket(10, new String[] {"f2"}));

    partitionField = List.of("TRUNCATE(f1, 3)");
    transforms = ExpressionUtil.partitionFiledToExpression(partitionField);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(transforms[0], Transforms.truncate(3, new String[] {"f1"}));

    partitionField = List.of("truncate(f1, 3)");
    transforms = ExpressionUtil.partitionFiledToExpression(partitionField);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(transforms[0], Transforms.truncate(3, new String[] {"f1"}));

    partitionField = List.of("month(order_date)", "BUCKET(account_number, 10)", "country");
    transforms = ExpressionUtil.partitionFiledToExpression(partitionField);
    Assertions.assertEquals(3, transforms.length);
    Assertions.assertEquals(transforms[0], Transforms.month("order_date"));
    Assertions.assertEquals(transforms[1], Transforms.bucket(10, new String[] {"account_number"}));
    Assertions.assertEquals(transforms[2], Transforms.identity(new String[] {"country"}));
  }

  @Test
  void testErrorOfPartitionFiledToExpression() {
    // test invalid partition field name
    Assertions.assertThrows(
        TrinoException.class,
        () -> {
          List<String> partitionField = List.of("12");
          ExpressionUtil.partitionFiledToExpression(partitionField);
        },
        "Error parsing partition field");

    // test no exists partition function name
    Assertions.assertThrows(
        TrinoException.class,
        () -> {
          List<String> partitionField = List.of("abs(f1)");
          ExpressionUtil.partitionFiledToExpression(partitionField);
        },
        "Error parsing partition field");

    // test error function arguments
    Assertions.assertThrows(
        TrinoException.class,
        () -> {
          List<String> partitionField = List.of("year(f1, f2)");
          ExpressionUtil.partitionFiledToExpression(partitionField);
        },
        "Error parsing partition field");

    // test error function arguments
    Assertions.assertThrows(
        TrinoException.class,
        () -> {
          List<String> partitionField = List.of("year(12)");
          ExpressionUtil.partitionFiledToExpression(partitionField);
        },
        "Error parsing partition field");

    Assertions.assertThrows(
        TrinoException.class,
        () -> {
          List<String> partitionField = List.of("buket(f1, f2)");
          ExpressionUtil.partitionFiledToExpression(partitionField);
        },
        "Error parsing partition field");
  }

  @Test
  void testExpressionToPartitionFiled() {
    Transform[] transforms = new Transform[] {Transforms.identity(new String[] {"f1"})};
    List<String> partitionFiled = ExpressionUtil.expressionToPartitionFiled(transforms);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(partitionFiled.get(0), "f1");

    transforms = new Transform[] {Transforms.year("f1")};
    partitionFiled = ExpressionUtil.expressionToPartitionFiled(transforms);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(partitionFiled.get(0), "year(f1)");

    transforms = new Transform[] {Transforms.month("f2")};
    partitionFiled = ExpressionUtil.expressionToPartitionFiled(transforms);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(partitionFiled.get(0), "month(f2)");

    transforms = new Transform[] {Transforms.day("f3")};
    partitionFiled = ExpressionUtil.expressionToPartitionFiled(transforms);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(partitionFiled.get(0), "day(f3)");

    transforms = new Transform[] {Transforms.hour("f4")};
    partitionFiled = ExpressionUtil.expressionToPartitionFiled(transforms);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(partitionFiled.get(0), "hour(f4)");

    transforms = new Transform[] {Transforms.bucket(10, new String[] {"f2"})};
    partitionFiled = ExpressionUtil.expressionToPartitionFiled(transforms);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(partitionFiled.get(0), "bucket(f2, 10)");

    transforms = new Transform[] {Transforms.truncate(3, new String[] {"f1"})};
    partitionFiled = ExpressionUtil.expressionToPartitionFiled(transforms);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(partitionFiled.get(0), "truncate(f1, 3)");

    transforms = new Transform[] {Transforms.truncate(3, new String[] {"f1"})};
    partitionFiled = ExpressionUtil.expressionToPartitionFiled(transforms);
    Assertions.assertEquals(1, transforms.length);
    Assertions.assertEquals(partitionFiled.get(0), "truncate(f1, 3)");

    transforms =
        new Transform[] {
          Transforms.month("order_date"),
          Transforms.bucket(10, new String[] {"account_number"}),
          Transforms.identity(new String[] {"country"})
        };
    partitionFiled = ExpressionUtil.expressionToPartitionFiled(transforms);
    Assertions.assertEquals(3, transforms.length);
    Assertions.assertEquals(partitionFiled.get(0), "month(order_date)");
    Assertions.assertEquals(partitionFiled.get(1), "bucket(account_number, 10)");
    Assertions.assertEquals(partitionFiled.get(2), "country");
  }

  @Test
  void testExpressionToSortOrderFiled() {
    SortOrder[] sortOrders = new SortOrder[] {SortOrders.ascending(NamedReference.field("f1"))};
    List<String> sortOrderFiled = ExpressionUtil.expressionToSortOrderFiled(sortOrders);
    Assertions.assertEquals(1, sortOrders.length);
    Assertions.assertEquals("f1", sortOrderFiled.get(0));

    sortOrders = new SortOrder[] {SortOrders.descending(NamedReference.field("f2"))};
    sortOrderFiled = ExpressionUtil.expressionToSortOrderFiled(sortOrders);
    Assertions.assertEquals(1, sortOrders.length);
    Assertions.assertEquals("f2 DESC", sortOrderFiled.get(0));

    sortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field("f1"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST)
        };
    sortOrderFiled = ExpressionUtil.expressionToSortOrderFiled(sortOrders);
    Assertions.assertEquals(1, sortOrders.length);
    Assertions.assertEquals("f1 ASC NULLS LAST", sortOrderFiled.get(0));

    sortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field("f2"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST)
        };
    sortOrderFiled = ExpressionUtil.expressionToSortOrderFiled(sortOrders);
    Assertions.assertEquals(1, sortOrders.length);
    Assertions.assertEquals("f2 DESC NULLS FIRST", sortOrderFiled.get(0));

    sortOrders =
        new SortOrder[] {
          SortOrders.ascending(NamedReference.field("f1")),
          SortOrders.descending(NamedReference.field("f2")),
          SortOrders.of(
              NamedReference.field("f3"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST),
          SortOrders.of(
              NamedReference.field("f4"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST)
        };
    sortOrderFiled = ExpressionUtil.expressionToSortOrderFiled(sortOrders);
    Assertions.assertEquals(4, sortOrders.length);
    Assertions.assertEquals("f1", sortOrderFiled.get(0));
    Assertions.assertEquals("f2 DESC", sortOrderFiled.get(1));
    Assertions.assertEquals("f3 ASC NULLS LAST", sortOrderFiled.get(2));
    Assertions.assertEquals("f4 DESC NULLS FIRST", sortOrderFiled.get(3));
  }

  @Test
  void testSortOrderFiledToExpression() {
    List<String> sortOrderFiled = List.of("f1");
    SortOrder[] sortOrders = ExpressionUtil.sortOrderFiledToExpression(sortOrderFiled);
    Assertions.assertEquals(1, sortOrders.length);
    Assertions.assertEquals(SortOrders.ascending(NamedReference.field("f1")), sortOrders[0]);

    sortOrderFiled = List.of("F2 desc");
    sortOrders = ExpressionUtil.sortOrderFiledToExpression(sortOrderFiled);
    Assertions.assertEquals(1, sortOrders.length);
    Assertions.assertEquals(SortOrders.descending(NamedReference.field("F2")), sortOrders[0]);

    sortOrderFiled = List.of("f1 ASC NULLS LAST");
    sortOrders = ExpressionUtil.sortOrderFiledToExpression(sortOrderFiled);
    Assertions.assertEquals(1, sortOrders.length);
    Assertions.assertEquals(
        SortOrders.of(NamedReference.field("f1"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST),
        sortOrders[0]);

    sortOrderFiled = List.of("f2 desc nulls first");
    sortOrders = ExpressionUtil.sortOrderFiledToExpression(sortOrderFiled);
    Assertions.assertEquals(1, sortOrders.length);
    Assertions.assertEquals(
        SortOrders.of(
            NamedReference.field("f2"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
        sortOrders[0]);

    sortOrderFiled = List.of("f1", "f2 DESC", "f3 ASC NULLS LAST", "F4 DESC NULLS FIRST");
    sortOrders = ExpressionUtil.sortOrderFiledToExpression(sortOrderFiled);
    Assertions.assertEquals(4, sortOrders.length);
    Assertions.assertEquals(SortOrders.ascending(NamedReference.field("f1")), sortOrders[0]);
    Assertions.assertEquals(SortOrders.descending(NamedReference.field("f2")), sortOrders[1]);
    Assertions.assertEquals(
        SortOrders.of(NamedReference.field("f3"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST),
        sortOrders[2]);
    Assertions.assertEquals(
        SortOrders.of(
            NamedReference.field("F4"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
        sortOrders[3]);

    sortOrderFiled = List.of("f1 ASC");
    sortOrders = ExpressionUtil.sortOrderFiledToExpression(sortOrderFiled);
    Assertions.assertEquals(1, sortOrders.length);
    Assertions.assertEquals(SortOrders.ascending(NamedReference.field("f1")), sortOrders[0]);
  }

  @Test
  void testErrorOfSortOrderFiledToExpression() {
    // test invalid sort order field name
    Assertions.assertThrows(
        TrinoException.class,
        () -> {
          List<String> sortOrderFields = List.of("12");
          ExpressionUtil.partitionFiledToExpression(sortOrderFields);
        },
        "Error parsing partition field");

    Assertions.assertThrows(
        TrinoException.class,
        () -> {
          List<String> sortOrderFields = List.of("f12", "1");
          ExpressionUtil.partitionFiledToExpression(sortOrderFields);
        },
        "Error parsing partition field");

    // test invalid sort order format
    Assertions.assertThrows(
        TrinoException.class,
        () -> {
          List<String> sortOrderFields = List.of("f12 dxxx");
          ExpressionUtil.partitionFiledToExpression(sortOrderFields);
        },
        "Error parsing partition field");

    Assertions.assertThrows(
        TrinoException.class,
        () -> {
          List<String> sortOrderFields = List.of("f12 asc nulls all");
          ExpressionUtil.partitionFiledToExpression(sortOrderFields);
        },
        "Error parsing partition field");
  }
}
