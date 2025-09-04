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
package org.apache.gravitino.stats;

import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionRange {

  @Test
  public void testPartitionRange() {
    SortOrder defaultSortOrder =
        SortOrders.of(NamedReference.MetadataField.PARTITION_NAME_FIELD, SortDirection.ASCENDING);

    PartitionRange range1 = PartitionRange.upTo("upper", PartitionRange.BoundType.OPEN);
    Assertions.assertTrue(range1.upperPartitionName().isPresent());
    Assertions.assertFalse(range1.lowerPartitionName().isPresent());
    Assertions.assertEquals("upper", range1.upperPartitionName().get());
    Assertions.assertEquals(defaultSortOrder, range1.comparator());
    Assertions.assertEquals(PartitionRange.BoundType.OPEN, range1.upperBoundType().get());

    PartitionRange range2 = PartitionRange.downTo("lower", PartitionRange.BoundType.CLOSED);
    Assertions.assertFalse(range2.upperPartitionName().isPresent());
    Assertions.assertTrue(range2.lowerPartitionName().isPresent());
    Assertions.assertEquals("lower", range2.lowerPartitionName().get());
    Assertions.assertEquals(defaultSortOrder, range2.comparator());
    Assertions.assertEquals(PartitionRange.BoundType.CLOSED, range2.lowerBoundType().get());

    PartitionRange range3 =
        PartitionRange.downTo("lower", PartitionRange.BoundType.CLOSED, defaultSortOrder);
    Assertions.assertTrue(range3.lowerPartitionName().isPresent());
    Assertions.assertFalse(range3.upperPartitionName().isPresent());
    Assertions.assertEquals("lower", range3.lowerPartitionName().get());
    Assertions.assertEquals(range3.comparator(), defaultSortOrder);
    Assertions.assertEquals(PartitionRange.BoundType.CLOSED, range3.lowerBoundType().get());

    PartitionRange range4 =
        PartitionRange.upTo("upper", PartitionRange.BoundType.OPEN, defaultSortOrder);
    Assertions.assertTrue(range4.upperPartitionName().isPresent());
    Assertions.assertFalse(range4.lowerPartitionName().isPresent());
    Assertions.assertEquals("upper", range4.upperPartitionName().get());
    Assertions.assertEquals(range4.comparator(), defaultSortOrder);
    Assertions.assertEquals(PartitionRange.BoundType.OPEN, range4.upperBoundType().get());

    PartitionRange range5 =
        PartitionRange.between(
            "lower",
            PartitionRange.BoundType.OPEN,
            "upper",
            PartitionRange.BoundType.CLOSED,
            defaultSortOrder);
    Assertions.assertTrue(range5.lowerPartitionName().isPresent());
    Assertions.assertTrue(range5.upperPartitionName().isPresent());
    Assertions.assertEquals("lower", range5.lowerPartitionName().get());
    Assertions.assertEquals("upper", range5.upperPartitionName().get());
    Assertions.assertEquals(defaultSortOrder, range5.comparator());
    Assertions.assertEquals(PartitionRange.BoundType.OPEN, range5.lowerBoundType().get());
    Assertions.assertEquals(PartitionRange.BoundType.CLOSED, range5.upperBoundType().get());

    PartitionRange range6 =
        PartitionRange.between(
            "lower", PartitionRange.BoundType.CLOSED, "upper", PartitionRange.BoundType.OPEN);
    Assertions.assertTrue(range6.lowerPartitionName().isPresent());
    Assertions.assertTrue(range6.upperPartitionName().isPresent());
    Assertions.assertEquals("lower", range6.lowerPartitionName().get());
    Assertions.assertEquals("upper", range6.upperPartitionName().get());
    Assertions.assertEquals(defaultSortOrder, range6.comparator());
    Assertions.assertEquals(PartitionRange.BoundType.CLOSED, range6.lowerBoundType().get());
    Assertions.assertEquals(PartitionRange.BoundType.OPEN, range6.upperBoundType().get());
  }
}
