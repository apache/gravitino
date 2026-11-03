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

import java.util.Map;
import org.apache.gravitino.rel.expressions.NamedReference;
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

    Transform[] weekStartPartitions = operations.parsePartitioning("toStartOfWeek(event_time)");
    Assertions.assertEquals(1, weekStartPartitions.length);
    assertFunctionTransform(weekStartPartitions[0], "toStartOfWeek", "event_time");

    Transform[] monthStartPartitions = operations.parsePartitioning("toStartOfMonth(event_time)");
    Assertions.assertEquals(1, monthStartPartitions.length);
    assertFunctionTransform(monthStartPartitions[0], "toStartOfMonth", "event_time");

    Transform[] quarterStartPartitions =
        operations.parsePartitioning("toStartOfQuarter(event_time)");
    Assertions.assertEquals(1, quarterStartPartitions.length);
    assertFunctionTransform(quarterStartPartitions[0], "toStartOfQuarter", "event_time");

    Transform[] yearStartPartitions = operations.parsePartitioning("toStartOfYear(event_time)");
    Assertions.assertEquals(1, yearStartPartitions.length);
    assertFunctionTransform(yearStartPartitions[0], "toStartOfYear", "event_time");

    // A native expression that cannot be structured returns an empty transform array. The raw
    // expression is instead exposed through the read-only partition-key property during load.
    Assertions.assertEquals(0, operations.parsePartitioning("cityHash64(user_id) % 16").length);
    Assertions.assertEquals(0, operations.parsePartitioning("toStartOfDay(event_time)").length);

    Transform[] identityPartitions = operations.parsePartitioning("metric_type");
    Assertions.assertEquals(1, identityPartitions.length);
    assertSingleFieldTransform(identityPartitions[0], Transforms.NAME_OF_IDENTITY, "metric_type");

    Transform[] tuplePartitions = operations.parsePartitioning("(toYYYYMM(ts), tenant_id)");
    Assertions.assertEquals(2, tuplePartitions.length);
    assertSingleFieldTransform(tuplePartitions[0], Transforms.NAME_OF_MONTH, "ts");
    assertSingleFieldTransform(tuplePartitions[1], Transforms.NAME_OF_IDENTITY, "tenant_id");

    Transform[] functionTuplePartitions =
        operations.parsePartitioning(
            "(toStartOfWeek(ts), toStartOfMonth(created_at), "
                + "toStartOfQuarter(created_at), toStartOfYear(ts))");
    Assertions.assertEquals(4, functionTuplePartitions.length);
    assertFunctionTransform(functionTuplePartitions[0], "toStartOfWeek", "ts");
    assertFunctionTransform(functionTuplePartitions[1], "toStartOfMonth", "created_at");
    assertFunctionTransform(functionTuplePartitions[2], "toStartOfQuarter", "created_at");
    assertFunctionTransform(functionTuplePartitions[3], "toStartOfYear", "ts");

    Assertions.assertEquals(0, operations.parsePartitioning("tuple()").length);
    Assertions.assertEquals(0, operations.parsePartitioning("  ").length);
  }

  @Test
  public void testNestedExpressionInsideKnownTransformIsNotMisrepresented() {
    // A nested expression inside toYear cannot be mapped to a single-field year transform, so the
    // whole partition key is treated as unsupported and returns an empty transform array rather
    // than misrepresenting it as year("f(x)").
    Assertions.assertEquals(0, operations.parsePartitioning("toYear(toString(event_time))").length);
    Assertions.assertEquals(
        0, operations.parsePartitioning("toStartOfMonth(toDate(event_time))").length);
    Assertions.assertEquals(
        0, operations.parsePartitioning("toStartOfQuarter(toDate(event_time))").length);
    Assertions.assertEquals(
        0, operations.parsePartitioning("toStartOfYear(toDate(event_time))").length);
  }

  @Test
  public void testBacktickQuotedColumnNameIsStructured() {
    // A backtick-quoted column name (which may contain special characters such as "-") is a real
    // column reference and must be structured rather than rejected.
    Transform[] monthPartitions = operations.parsePartitioning("toYYYYMM(`event-time`)");
    Assertions.assertEquals(1, monthPartitions.length);
    assertSingleFieldTransform(monthPartitions[0], Transforms.NAME_OF_MONTH, "event-time");

    Transform[] weekStartPartitions = operations.parsePartitioning("toStartOfWeek(`event-time`)");
    Assertions.assertEquals(1, weekStartPartitions.length);
    assertFunctionTransform(weekStartPartitions[0], "toStartOfWeek", "event-time");

    Transform[] monthStartPartitions = operations.parsePartitioning("toStartOfMonth(`event-time`)");
    Assertions.assertEquals(1, monthStartPartitions.length);
    assertFunctionTransform(monthStartPartitions[0], "toStartOfMonth", "event-time");

    Transform[] quarterStartPartitions =
        operations.parsePartitioning("toStartOfQuarter(`event-time`)");
    Assertions.assertEquals(1, quarterStartPartitions.length);
    assertFunctionTransform(quarterStartPartitions[0], "toStartOfQuarter", "event-time");

    Transform[] yearStartPartitions = operations.parsePartitioning("toStartOfYear(`event-time`)");
    Assertions.assertEquals(1, yearStartPartitions.length);
    assertFunctionTransform(yearStartPartitions[0], "toStartOfYear", "event-time");

    Transform[] identityPartitions = operations.parsePartitioning("`event-time`");
    Assertions.assertEquals(1, identityPartitions.length);
    assertSingleFieldTransform(identityPartitions[0], Transforms.NAME_OF_IDENTITY, "event-time");
  }

  @Test
  public void testWeekModeAndTimezoneFormsRemainUnstructured() {
    Assertions.assertEquals(0, operations.parsePartitioning("toStartOfWeek(event_time, 1)").length);
    Assertions.assertEquals(
        0, operations.parsePartitioning("toStartOfWeek(event_time, 1, 'Asia/Shanghai')").length);
  }

  @Test
  public void testQuarterAndYearAdditionalArgumentsRemainUnstructured() {
    Assertions.assertEquals(
        0, operations.parsePartitioning("toStartOfQuarter(event_time, 1)").length);
    Assertions.assertEquals(0, operations.parsePartitioning("toStartOfYear(event_time, 1)").length);
  }

  @Test
  public void testPureTypeConversionIsUnwrapped() {
    Map<String, String> dateColumns = Map.of("event_time", "Date");
    Transform[] monthPartitions =
        operations.parsePartitioning("toYYYYMM(toDate(event_time))", dateColumns);
    Assertions.assertEquals(1, monthPartitions.length);
    assertSingleFieldTransform(monthPartitions[0], Transforms.NAME_OF_MONTH, "event_time");

    Map<String, String> dateTimeColumns = Map.of("ts", "DateTime");
    Transform[] yearPartitions =
        operations.parsePartitioning("toYear(toDateTime(ts))", dateTimeColumns);
    Assertions.assertEquals(1, yearPartitions.length);
    assertSingleFieldTransform(yearPartitions[0], Transforms.NAME_OF_YEAR, "ts");
  }

  @Test
  public void testNonEquivalentExpressionsRemainUnstructured() {
    // A String column requires the toDate cast, so it must not be unwrapped.
    Map<String, String> stringColumns = Map.of("event_time", "String");
    Assertions.assertEquals(
        0, operations.parsePartitioning("toYYYYMM(toDate(event_time))", stringColumns).length);

    // DateTime64 -> Date/DateTime is a narrowing conversion that can overflow, so it must not be
    // unwrapped.
    Map<String, String> dateTime64Columns = Map.of("ts", "DateTime64(0)");
    Assertions.assertEquals(
        0, operations.parsePartitioning("toYear(toDateTime(ts))", dateTime64Columns).length);

    // Date -> DateTime is a narrowing conversion (Date range exceeds DateTime), so it must not be
    // unwrapped.
    Map<String, String> dateForDateTimeColumns = Map.of("ts", "Date");
    Assertions.assertEquals(
        0, operations.parsePartitioning("toYear(toDateTime(ts))", dateForDateTimeColumns).length);

    // toDate over DateTime is not unwrapped because the effective timezone can shift the epoch
    // boundary (e.g. timestamp 0 is 1970 in UTC but 1969 in America/Los_Angeles).
    Map<String, String> dateTimeForDateColumns = Map.of("ts", "DateTime");
    Assertions.assertEquals(
        0, operations.parsePartitioning("toYear(toDate(ts))", dateTimeForDateColumns).length);

    Map<String, String> dateColumns = Map.of("statis_ymd", "Date");
    // An arithmetic offset changes the semantics, so it must not be unwrapped.
    Assertions.assertEquals(
        0, operations.parsePartitioning("toYYYYMM(toDate(statis_ymd + 1))", dateColumns).length);

    Map<String, String> dateTimeColumns = Map.of("ts", "DateTime");
    // A nested granularity function changes the granularity, so it must not be unwrapped.
    Assertions.assertEquals(
        0, operations.parsePartitioning("toDate(toStartOfMonth(x))", dateColumns).length);
    // A cast function (toString) is not a pure conversion.
    Assertions.assertEquals(
        0, operations.parsePartitioning("toYear(toString(ts))", dateTimeColumns).length);
  }

  @Test
  public void testPureTypeConversionRoundTrips() {
    Map<String, String> dateColumns = Map.of("event_time", "Date");
    Transform[] parsed = operations.parsePartitioning("toYYYYMM(toDate(event_time))", dateColumns);
    Assertions.assertEquals(1, parsed.length);
    Assertions.assertEquals(
        "toYYYYMM(`event_time`)", ClickHouseTableSqlUtils.toPartitionExpression(parsed[0]));
  }

  private void assertFunctionTransform(
      Transform transform, String expectedName, String expectedColumn) {
    Assertions.assertEquals(expectedName, transform.name());
    Assertions.assertEquals(1, transform.arguments().length);
    Assertions.assertEquals(NamedReference.field(expectedColumn), transform.arguments()[0]);
  }

  private void assertSingleFieldTransform(
      Transform transform, String expectedName, String expectedColumn) {
    Assertions.assertEquals(expectedName, transform.name());
    Assertions.assertArrayEquals(
        new String[] {expectedColumn}, ((Transform.SingleFieldTransform) transform).fieldName());
  }
}
