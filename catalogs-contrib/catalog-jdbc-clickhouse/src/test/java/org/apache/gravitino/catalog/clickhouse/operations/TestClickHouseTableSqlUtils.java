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
package org.apache.gravitino.catalog.clickhouse.operations;

import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.escapeSingleQuotes;

import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestClickHouseTableSqlUtils {

  @Test
  void testEscapeSingleQuotesNormal() {
    Assertions.assertEquals("my_db", escapeSingleQuotes("my_db"));
  }

  @Test
  void testEscapeSingleQuotesWithQuote() {
    Assertions.assertEquals("test''db", escapeSingleQuotes("test'db"));
  }

  @Test
  void testEscapeSingleQuotesMultiple() {
    Assertions.assertEquals("a''b''c", escapeSingleQuotes("a'b'c"));
  }

  @Test
  void testEscapeSingleQuotesEmpty() {
    Assertions.assertEquals("", escapeSingleQuotes(""));
  }

  // ---------------------------------------------------------------------------
  // toPartitionExpression round-trip with day/month/year transforms
  // ---------------------------------------------------------------------------

  @Test
  void testToPartitionExpressionDay() {
    Transform day = Transforms.day("dt");
    Assertions.assertEquals("toYYYYMMDD(`dt`)", ClickHouseTableSqlUtils.toPartitionExpression(day));
  }

  @Test
  void testToPartitionExpressionMonth() {
    Transform month = Transforms.month("dt");
    Assertions.assertEquals("toYYYYMM(`dt`)", ClickHouseTableSqlUtils.toPartitionExpression(month));
  }

  @Test
  void testToPartitionExpressionYear() {
    Transform year = Transforms.year("dt");
    Assertions.assertEquals("toYear(`dt`)", ClickHouseTableSqlUtils.toPartitionExpression(year));
  }

  @Test
  void testToPartitionExpressionIdentity() {
    Transform identity = Transforms.identity(new String[] {"col"});
    Assertions.assertEquals("`col`", ClickHouseTableSqlUtils.toPartitionExpression(identity));
  }

  // ---------------------------------------------------------------------------
  // extractInnermostField — nested function unwrapping
  // ---------------------------------------------------------------------------

  @Test
  void testExtractInnermostFieldSimple() {
    Assertions.assertEquals("dt", ClickHouseTableSqlUtils.extractInnermostField("dt"));
  }

  @Test
  void testExtractInnermostFieldBacktickQuoted() {
    Assertions.assertEquals("dt", ClickHouseTableSqlUtils.extractInnermostField("`dt`"));
  }

  @Test
  void testExtractInnermostFieldSingleWrapper() {
    Assertions.assertEquals("dt", ClickHouseTableSqlUtils.extractInnermostField("toDate(dt)"));
  }

  @Test
  void testExtractInnermostFieldNested() {
    Assertions.assertEquals(
        "dt", ClickHouseTableSqlUtils.extractInnermostField("toYYYYMM(toDate(dt))"));
  }

  @Test
  void testExtractInnermostFieldNestedYear() {
    Assertions.assertEquals(
        "dt", ClickHouseTableSqlUtils.extractInnermostField("toYear(toDate(dt))"));
  }

  @Test
  void testExtractInnermostFieldNullOnComplex() {
    Assertions.assertNull(ClickHouseTableSqlUtils.extractInnermostField("intDiv(col, 100)"));
  }

  // ---------------------------------------------------------------------------
  // parsePartitioning — round-trip verification
  // ---------------------------------------------------------------------------

  @Test
  void testParsePartitioningRoundTripDay() {
    Transform[] parsed = ClickHouseTableSqlUtils.parsePartitioning("toYYYYMMDD(dt)");
    Assertions.assertEquals(1, parsed.length);
    Assertions.assertEquals("day", parsed[0].name());
    Assertions.assertEquals("dt", ((NamedReference) parsed[0].arguments()[0]).fieldName()[0]);
    Assertions.assertEquals(
        "toYYYYMMDD(`dt`)", ClickHouseTableSqlUtils.toPartitionExpression(parsed[0]));
  }

  @Test
  void testParsePartitioningRoundTripMonth() {
    Transform[] parsed = ClickHouseTableSqlUtils.parsePartitioning("toYYYYMM(dt)");
    Assertions.assertEquals(1, parsed.length);
    Assertions.assertEquals("month", parsed[0].name());
    Assertions.assertEquals(
        "toYYYYMM(`dt`)", ClickHouseTableSqlUtils.toPartitionExpression(parsed[0]));
  }

  @Test
  void testParsePartitioningRoundTripYear() {
    Transform[] parsed = ClickHouseTableSqlUtils.parsePartitioning("toYear(dt)");
    Assertions.assertEquals(1, parsed.length);
    Assertions.assertEquals("year", parsed[0].name());
    Assertions.assertEquals(
        "toYear(`dt`)", ClickHouseTableSqlUtils.toPartitionExpression(parsed[0]));
  }

  @Test
  void testParsePartitioningNestedToYYYYMM() {
    Transform[] parsed = ClickHouseTableSqlUtils.parsePartitioning("toYYYYMM(toDate(dt))");
    Assertions.assertEquals(1, parsed.length);
    Assertions.assertEquals("month", parsed[0].name());
    Assertions.assertEquals("dt", ((NamedReference) parsed[0].arguments()[0]).fieldName()[0]);
  }

  @Test
  void testParsePartitioningNestedToYear() {
    Transform[] parsed = ClickHouseTableSqlUtils.parsePartitioning("toYear(toDate(dt))");
    Assertions.assertEquals(1, parsed.length);
    Assertions.assertEquals("year", parsed[0].name());
    Assertions.assertEquals("dt", ((NamedReference) parsed[0].arguments()[0]).fieldName()[0]);
  }
}
