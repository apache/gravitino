/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.catalog.clickhouse.operations;

import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestClickHouseTableOperationsPartitioning {

  private final ClickHouseTableOperations operations = new ClickHouseTableOperations();

  @Test
  public void testParseSupportedPartitionExpressions() {
    Transform[] dayPartitions = operations.parsePartitioning("toDate(event_time)");
    Assertions.assertEquals(1, dayPartitions.length);
    assertSingleFieldTransform(dayPartitions[0], Transforms.NAME_OF_DAY, "event_time");

    Transform[] monthPartitions = operations.parsePartitioning("toYYYYMM(event_time)");
    Assertions.assertEquals(1, monthPartitions.length);
    assertSingleFieldTransform(monthPartitions[0], Transforms.NAME_OF_MONTH, "event_time");

    Transform[] yearPartitions = operations.parsePartitioning("toYear(event_time)");
    Assertions.assertEquals(1, yearPartitions.length);
    assertSingleFieldTransform(yearPartitions[0], Transforms.NAME_OF_YEAR, "event_time");

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> operations.parsePartitioning("cityHash64(user_id) % 16"));

    Transform[] identityPartitions = operations.parsePartitioning("metric_type");
    Assertions.assertEquals(1, identityPartitions.length);
    assertSingleFieldTransform(identityPartitions[0], Transforms.NAME_OF_IDENTITY, "metric_type");

    Transform[] tuplePartitions = operations.parsePartitioning("(toYYYYMM(ts), tenant_id)");
    Assertions.assertEquals(2, tuplePartitions.length);
    assertSingleFieldTransform(tuplePartitions[0], Transforms.NAME_OF_MONTH, "ts");
    assertSingleFieldTransform(tuplePartitions[1], Transforms.NAME_OF_IDENTITY, "tenant_id");

    Assertions.assertEquals(0, operations.parsePartitioning("tuple()").length);
    Assertions.assertEquals(0, operations.parsePartitioning("  ").length);
  }

  private void assertSingleFieldTransform(
      Transform transform, String expectedName, String expectedColumn) {
    Assertions.assertEquals(expectedName, transform.name());
    Assertions.assertArrayEquals(
        new String[] {expectedColumn}, ((Transform.SingleFieldTransform) transform).fieldName());
  }
}
