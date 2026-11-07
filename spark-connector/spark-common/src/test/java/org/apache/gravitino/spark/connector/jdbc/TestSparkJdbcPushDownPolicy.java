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

package org.apache.gravitino.spark.connector.jdbc;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Cast;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.Max;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScanBuilder;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;

/** Expression-level pushdown decisions for JDBC tables. */
public class TestSparkJdbcPushDownPolicy {

  private static final String URL = "jdbc:postgresql://localhost/test";
  private static final JdbcDialect DIALECT = JdbcDialects.get(URL);

  @Test
  // Cast(Expression, DataType) is deprecated in Spark 4 but is the only constructor in Spark 3.5.
  @SuppressWarnings("deprecation")
  void testOnlyExpressionsWithSensitiveValuesStayInSpark() {
    SparkJdbcPushDownPolicy policy = policy();
    Assertions.assertTrue(policy.canPush(eq(col("id"), lit(1, DataTypes.IntegerType))));
    Assertions.assertTrue(policy.canPush(eq(col("ID"), lit(1, DataTypes.IntegerType))));
    Assertions.assertFalse(policy.canPush(eq(col("s"), lit("a", DataTypes.StringType))));
    Assertions.assertFalse(policy.canPush(eq(col("v"), lit("a", DataTypes.StringType))));
    Assertions.assertFalse(policy.canPush(eq(col("b"), lit(new byte[] {1}, DataTypes.BinaryType))));
    Assertions.assertFalse(policy.canPush(col("u")));
    Assertions.assertFalse(policy.canPush(col("j")));
    // Array elements compare with the element collation at the source.
    Assertions.assertFalse(policy.canPush(col("tags")));
    // A string cast or literal stays in Spark even over a numeric column.
    Assertions.assertFalse(
        policy.canPush(
            new Predicate(
                ">",
                new Expression[] {
                  new Cast(col("id"), DataTypes.StringType), lit("9", DataTypes.StringType)
                })));
    // One unsafe side keeps the whole conjunction in Spark; Spark splits top-level ANDs itself.
    Assertions.assertFalse(
        policy.canPush(
            new And(
                eq(col("id"), lit(1, DataTypes.IntegerType)),
                eq(col("s"), lit("a", DataTypes.StringType)))));
    Assertions.assertTrue(
        policy.canPush(
            eq(col("ts"), lit(LocalDateTime.of(2024, 1, 2, 3, 4), DataTypes.TimestampNTZType)))
            == (DIALECT.compileValue(LocalDateTime.of(2000, 1, 1, 0, 0)) instanceof String));
    Assertions.assertTrue(policy.canPush(col("ts")));
  }

  @Test
  void testAggregatesStayInSparkWhenReaderTypeDiffers() {
    SparkJdbcPushDownPolicy policy = policy();
    // "tz" is NTZ for the reader but TIMESTAMP for Spark, as on Spark 3.5 PostgreSQL.
    Assertions.assertTrue(policy.canPush(col("tz")));
    Assertions.assertFalse(policy.canPushAggregate(new Max(col("tz"))));
    Assertions.assertTrue(policy.canPushAggregate(new Max(col("id"))));
    Assertions.assertTrue(policy.canPushAggregate(new CountStar()));
    Assertions.assertFalse(policy.canPushAggregate(new Max(col("s"))));
  }

  @Test
  void testScanBuilderKeepsUnsafeExpressionsAndPushesTheRest() {
    SparkJdbcPushDownPolicy policy = policy();
    JDBCOptions options =
        new JDBCOptions(
            new HashMap<String, String>()
                .updated("url", URL)
                .updated("dbtable", "t")
                .updated("driver", "org.postgresql.Driver")
                .updated("pushDownPredicate", "true")
                .updated("pushDownLimit", "true"));
    SparkJdbcScanBuilder builder =
        new SparkJdbcScanBuilder(
            new JDBCScanBuilder(mock(SparkSession.class, RETURNS_DEEP_STUBS), sparkSchema(), options),
            policy);
    Predicate safe = eq(col("id"), lit(1, DataTypes.IntegerType));
    Predicate unsafe = eq(col("s"), lit("a", DataTypes.StringType));

    List<Predicate> postScan = Arrays.asList(builder.pushPredicates(new Predicate[] {safe, unsafe}));
    Assertions.assertEquals(List.of(unsafe), postScan);
    Assertions.assertEquals(List.of(safe), Arrays.asList(builder.pushedPredicates()));
    Assertions.assertFalse(builder.pushTopN(new SortOrder[] {sort(col("s"))}, 1));
    Assertions.assertTrue(builder.pushTopN(new SortOrder[] {sort(col("id"))}, 1));
    Assertions.assertTrue(builder.pushLimit(10));
    Aggregation stringAggregate =
        new Aggregation(new AggregateFunc[] {new Max(col("s"))}, new Expression[0]);
    Assertions.assertFalse(builder.pushAggregation(stringAggregate));
    Assertions.assertFalse(builder.supportCompletePushDown(stringAggregate));
    Aggregation groupByString =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {col("s")});
    Assertions.assertFalse(builder.pushAggregation(groupByString));
  }

  private static SparkJdbcPushDownPolicy policy() {
    Column[] columns = {
      column("id", Types.IntegerType.get()),
      column("s", Types.StringType.get()),
      column("v", Types.VarCharType.of(10)),
      column("b", Types.BinaryType.get()),
      column("u", Types.UUIDType.get()),
      column("j", Types.ExternalType.of("jsonb")),
      column("tags", Types.ListType.nullable(Types.StringType.get())),
      column("ts", Types.TimestampType.withoutTimeZone()),
      column("tz", Types.TimestampType.withTimeZone())
    };
    // The reader sees timestamptz as NTZ, as Spark 3.5 does under preferTimestampNTZ.
    StructType reader = new StructType();
    for (StructField field : sparkSchema().fields()) {
      reader =
          reader.add(field.name().equals("tz") ? field.copy("tz", DataTypes.TimestampNTZType, true, field.metadata()) : field);
    }
    return new SparkJdbcPushDownPolicy(columns, reader, sparkSchema(), DIALECT);
  }

  private static StructType sparkSchema() {
    return new StructType()
        .add("id", DataTypes.IntegerType)
        .add("s", DataTypes.StringType)
        .add("v", DataTypes.StringType)
        .add("b", DataTypes.BinaryType)
        .add("u", DataTypes.StringType)
        .add("j", DataTypes.StringType)
        .add("tags", DataTypes.createArrayType(DataTypes.StringType))
        .add("ts", DataTypes.TimestampNTZType)
        .add("tz", DataTypes.TimestampType);
  }

  private static Column column(String name, Type type) {
    return Column.of(name, type);
  }

  private static Expression col(String name) {
    return Expressions.column(name);
  }

  private static <T> LiteralValue<T> lit(T value, DataType type) {
    return new LiteralValue<>(value, type);
  }

  private static Predicate eq(Expression left, Expression right) {
    return new Predicate("=", new Expression[] {left, right});
  }

  private static SortOrder sort(Expression expression) {
    return Expressions.sort(expression, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
  }
}
