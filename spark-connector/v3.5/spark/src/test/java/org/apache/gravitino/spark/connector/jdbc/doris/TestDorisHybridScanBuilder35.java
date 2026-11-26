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

package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.sql.Timestamp;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScanBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests the coordination of native and JDBC scan delegates. */
public class TestDorisHybridScanBuilder35 {
  private final ScanBuilder nativeBuilder =
      mock(
          ScanBuilder.class,
          withSettings()
              .extraInterfaces(
                  SupportsPushDownRequiredColumns.class, SupportsPushDownV2Filters.class));
  private final SupportsPushDownRequiredColumns nativeColumns =
      (SupportsPushDownRequiredColumns) nativeBuilder;
  private final SupportsPushDownV2Filters nativeFilters = (SupportsPushDownV2Filters) nativeBuilder;
  private final JDBCScanBuilder jdbcBuilder = mock(JDBCScanBuilder.class);
  private final JDBCScan jdbcScan = mock(JDBCScan.class);

  @Test
  void testRequiredTimestampUsesJdbcWithoutPruningNativeSchema() {
    for (DataType type : new DataType[] {DataTypes.TimestampType, DataTypes.TimestampNTZType}) {
      DorisHybridScanBuilder35 builder = builder(new StructType().add("EVENT_TIME", type));
      StructType required = new StructType().add("event_time", type);
      builder.pruneColumns(required);
      assertSame(jdbcScan, builder.build());
      verify(jdbcBuilder).pruneColumns(required);
      verify(nativeColumns, never()).pruneColumns(any());
    }
  }

  @ParameterizedTest(name = "nativeRejects={0}")
  @ValueSource(booleans = {true, false})
  void testPredicatesRejectedByEitherDelegateRemainResidual(boolean nativeRejects) {
    DorisHybridScanBuilder35 builder = builder(new StructType());
    Predicate supported = predicate("id", 1);
    Predicate residual = predicate("name", "one");
    if (nativeRejects) {
      when(nativeFilters.pushPredicates(any(Predicate[].class)))
          .thenReturn(new Predicate[] {residual});
    } else {
      when(jdbcBuilder.pushPredicates(any(Predicate[].class)))
          .thenReturn(new Predicate[] {residual});
    }
    assertArrayEquals(
        new Predicate[] {residual}, builder.pushPredicates(new Predicate[] {supported, residual}));
    assertArrayEquals(new Predicate[] {supported}, builder.pushedPredicates());
    verify(jdbcBuilder).pushPredicates(new Predicate[] {supported, residual});
    verify(nativeFilters)
        .pushPredicates(
            nativeRejects ? new Predicate[] {supported, residual} : new Predicate[] {supported});
    StructType required = new StructType().add("id", DataTypes.IntegerType);
    builder.pruneColumns(required);
    verify(nativeColumns).pruneColumns(required);
    verify(jdbcBuilder).pruneColumns(required);
    Scan scan = mock(Scan.class);
    when(nativeBuilder.build()).thenReturn(scan);
    assertSame(scan, builder.build());
  }

  @Test
  void testLimitRoutesToJdbcAndOffsetDoesNotMakeLimitNonpositive() {
    DorisHybridScanBuilder35 builder = builder(new StructType());
    when(jdbcBuilder.pushLimit(10)).thenReturn(true);
    assertTrue(builder.pushLimit(10));
    assertFalse(builder.pushOffset(10));
    assertSame(jdbcScan, builder.build());
  }

  @Test
  void testJdbcAggregateColumnsAreNotPrunedAgainstNativeSchema() {
    DorisHybridScanBuilder35 builder = builder(new StructType());
    StructType output = new StructType().add("count(*)", DataTypes.LongType);
    when(jdbcBuilder.pushAggregation(null)).thenReturn(true);
    assertTrue(builder.pushAggregation(null));
    builder.pruneColumns(output);
    assertSame(jdbcScan, builder.build());
    verify(jdbcBuilder).pruneColumns(output);
    verify(nativeColumns, never()).pruneColumns(any());
  }

  @Test
  void testTimestampPredicateRemainsResidualBeforeScanSelection() {
    DorisHybridScanBuilder35 builder =
        builder(new StructType().add("event_time", DataTypes.TimestampType));
    Predicate timestamp =
        new Predicate(
            ">",
            new Expression[] {
              Expressions.column("event_time"),
              Expressions.literal(Timestamp.valueOf("2026-01-01 00:00:00"))
            });
    Predicate ordinary = predicate("id", 1);
    Predicate nullCheck =
        new Predicate("IS_NOT_NULL", new Expression[] {Expressions.column("event_time")});
    assertArrayEquals(
        new Predicate[] {timestamp},
        builder.pushPredicates(new Predicate[] {timestamp, ordinary, nullCheck}));
    Predicate[] pushed = new Predicate[] {ordinary, nullCheck};
    verify(nativeFilters).pushPredicates(pushed);
    verify(jdbcBuilder).pushPredicates(pushed);
    assertArrayEquals(pushed, builder.pushedPredicates());
  }

  private DorisHybridScanBuilder35 builder(StructType schema) {
    when(jdbcBuilder.build()).thenReturn(jdbcScan);
    when(nativeFilters.pushPredicates(any(Predicate[].class))).thenReturn(new Predicate[0]);
    when(jdbcBuilder.pushPredicates(any(Predicate[].class))).thenReturn(new Predicate[0]);
    return new DorisHybridScanBuilder35(nativeBuilder, jdbcBuilder, schema);
  }

  private static Predicate predicate(String column, Object value) {
    return new Predicate(
        "=", new Expression[] {Expressions.column(column), Expressions.literal(value)});
  }
}
