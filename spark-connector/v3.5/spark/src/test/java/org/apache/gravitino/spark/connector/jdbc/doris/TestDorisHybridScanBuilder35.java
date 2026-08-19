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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/** Tests lane selection and capability coordination for the Doris Spark 3.5 reader. */
public class TestDorisHybridScanBuilder35 {

  @Test
  void testDetailReadUsesNativeAndLimitSelectsJdbc() {
    RecordingBuilder nativeBuilder = new RecordingBuilder();
    RecordingBuilder jdbcBuilder = new RecordingBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, jdbcBuilder, false, Collections.emptySet());

    assertSame(nativeBuilder.scan, builder.build());
    assertTrue(builder.pushLimit(10));
    assertSame(jdbcBuilder.scan, builder.build());
  }

  @Test
  void testNormalizedRequiredColumnSelectsJdbc() {
    RecordingBuilder nativeBuilder = new RecordingBuilder();
    RecordingBuilder jdbcBuilder = new RecordingBuilder();
    DorisHybridScanBuilder35 builder =
        new DorisHybridScanBuilder35(nativeBuilder, jdbcBuilder, false, ImmutableSet.of("payload"));

    builder.pruneColumns(
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("payload", DataTypes.StringType, true)
            }));

    assertSame(jdbcBuilder.scan, builder.build());
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

    @Override
    public Scan build() {
      return scan;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {}

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
      return new Predicate[0];
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
    public boolean isPartiallyPushed() {
      return false;
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
    public boolean pushOffset(int offset) {
      return true;
    }
  }
}
