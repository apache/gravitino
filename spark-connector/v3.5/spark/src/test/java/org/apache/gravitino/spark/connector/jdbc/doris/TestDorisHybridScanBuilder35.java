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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/** Tests the coordination of native and JDBC scan delegates. */
public class TestDorisHybridScanBuilder35 {
  @Test
  void testDetailReadDelegatesColumnsAndSelectsNative() {
    RecordingBuilder nativeBuilder = new RecordingBuilder();
    RecordingBuilder jdbcBuilder = new RecordingBuilder();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("event_time", DataTypes.TimestampType);
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, jdbcBuilder, schema);
    StructType required = new StructType().add("id", DataTypes.IntegerType);

    builder.pruneColumns(required);
    assertSame(required, nativeBuilder.required);
    assertSame(required, jdbcBuilder.required);
    assertSame(nativeBuilder.scan, builder.build());
  }

  @Test
  void testRequiredTimestampUsesJdbcWithoutPruningNativeSchema() {
    for (DataType type : new DataType[] {DataTypes.TimestampType, DataTypes.TimestampNTZType}) {
      RecordingBuilder nativeBuilder = new RecordingBuilder();
      RecordingBuilder jdbcBuilder = new RecordingBuilder();
      StructType schema = new StructType().add("EVENT_TIME", type);
      StructType required = new StructType().add("event_time", type);
      DorisHybridScanBuilder35 builder =
          new DorisHybridScanBuilder35(nativeBuilder, jdbcBuilder, schema);

      builder.pruneColumns(required);

      assertSame(jdbcBuilder.scan, builder.build());
      assertSame(required, jdbcBuilder.required);
      assertNull(nativeBuilder.required);
    }
  }

  @Test
  void testPredicatesRejectedByEitherDelegateRemainResidual() {
    RecordingBuilder nativeBuilder = new RecordingBuilder();
    RecordingBuilder jdbcBuilder = new RecordingBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, jdbcBuilder, new StructType());
    Predicate supported =
        new Predicate("=", new Expression[] {Expressions.column("id"), Expressions.literal(1)});
    Predicate residual =
        new Predicate(
            "=", new Expression[] {Expressions.column("name"), Expressions.literal("one")});
    nativeBuilder.residual = new Predicate[] {residual};

    assertArrayEquals(
        new Predicate[] {residual}, builder.pushPredicates(new Predicate[] {supported, residual}));
    assertArrayEquals(new Predicate[] {supported}, builder.pushedPredicates());
    assertArrayEquals(new Predicate[] {supported, residual}, jdbcBuilder.received);
  }

  @Test
  void testJdbcResidualIsNotPushedToNative() {
    RecordingBuilder nativeBuilder = new RecordingBuilder();
    RecordingBuilder jdbcBuilder = new RecordingBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, jdbcBuilder, new StructType());
    Predicate predicate =
        new Predicate("=", new Expression[] {Expressions.column("id"), Expressions.literal(1)});
    jdbcBuilder.residual = new Predicate[] {predicate};

    assertArrayEquals(
        new Predicate[] {predicate}, builder.pushPredicates(new Predicate[] {predicate}));
    assertArrayEquals(new Predicate[0], nativeBuilder.received);
    assertArrayEquals(new Predicate[] {predicate}, jdbcBuilder.received);
    assertArrayEquals(new Predicate[0], builder.pushedPredicates());
  }

  @Test
  void testLimitRoutesToJdbcAndOffsetDoesNotMakeLimitNonpositive() {
    RecordingBuilder nativeBuilder = new RecordingBuilder();
    RecordingBuilder jdbcBuilder = new RecordingBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, jdbcBuilder, new StructType());

    assertTrue(builder.pushLimit(10));
    assertFalse(builder.pushOffset(10));
    assertSame(jdbcBuilder.scan, builder.build());
  }

  @Test
  void testJdbcAggregateColumnsAreNotPrunedAgainstNativeSchema() {
    RecordingBuilder nativeBuilder = new RecordingBuilder();
    RecordingBuilder jdbcBuilder = new RecordingBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, jdbcBuilder, new StructType());
    StructType aggregateOutput = new StructType().add("count(*)", DataTypes.LongType);

    assertTrue(builder.pushAggregation(null));
    builder.pruneColumns(aggregateOutput);

    assertSame(jdbcBuilder.scan, builder.build());
    assertSame(aggregateOutput, jdbcBuilder.required);
    assertNull(nativeBuilder.required);
  }

  @Test
  void testTimestampPredicateRemainsResidualBeforeScanSelection() {
    RecordingBuilder nativeBuilder = new RecordingBuilder();
    RecordingBuilder jdbcBuilder = new RecordingBuilder();
    StructType schema = new StructType().add("event_time", DataTypes.TimestampType);
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, jdbcBuilder, schema);
    Predicate predicate =
        new Predicate(
            ">",
            new Expression[] {
              Expressions.column("event_time"),
              Expressions.literal(Timestamp.valueOf("2026-01-01 00:00:00"))
            });

    assertArrayEquals(
        new Predicate[] {predicate}, builder.pushPredicates(new Predicate[] {predicate}));
    assertArrayEquals(new Predicate[0], nativeBuilder.received);
    assertArrayEquals(new Predicate[0], jdbcBuilder.received);
  }

  @Test
  void testTimestampNullCheckCanBePushed() {
    RecordingBuilder nativeBuilder = new RecordingBuilder();
    RecordingBuilder jdbcBuilder = new RecordingBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(
            nativeBuilder,
            jdbcBuilder,
            new StructType().add("event_time", DataTypes.TimestampType));
    Predicate predicate =
        new Predicate("IS_NOT_NULL", new Expression[] {Expressions.column("event_time")});

    assertArrayEquals(new Predicate[0], builder.pushPredicates(new Predicate[] {predicate}));
    assertArrayEquals(new Predicate[] {predicate}, nativeBuilder.received);
    assertArrayEquals(new Predicate[] {predicate}, jdbcBuilder.received);
  }

  private static final class RecordingBuilder
      implements ScanBuilder,
          SupportsPushDownRequiredColumns,
          SupportsPushDownV2Filters,
          SupportsPushDownLimit,
          SupportsPushDownAggregates,
          SupportsPushDownTopN,
          SupportsPushDownOffset {
    private final Scan scan = () -> new StructType();
    private Predicate[] residual = new Predicate[0];
    private Predicate[] received = new Predicate[0];
    private StructType required;

    @Override
    public Scan build() {
      return scan;
    }

    @Override
    public void pruneColumns(StructType schema) {
      required = schema;
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
      received = predicates;
      return residual;
    }

    @Override
    public Predicate[] pushedPredicates() {
      return new Predicate[0];
    }

    @Override
    public boolean pushLimit(int limit) {
      return true;
    }

    @Override
    public boolean supportCompletePushDown(Aggregation aggregation) {
      return true;
    }

    @Override
    public boolean pushAggregation(Aggregation aggregation) {
      return true;
    }

    @Override
    public boolean pushTopN(SortOrder[] orders, int limit) {
      return true;
    }

    @Override
    public boolean isPartiallyPushed() {
      return false;
    }

    @Override
    public boolean pushOffset(int offset) {
      return true;
    }
  }
}
