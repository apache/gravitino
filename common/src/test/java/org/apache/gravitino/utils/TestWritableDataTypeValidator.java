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
package org.apache.gravitino.utils;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestWritableDataTypeValidator {

  @ParameterizedTest
  @MethodSource("invalidExpressions")
  public void testRejectNullExpressionArraysAndAssignments(Expression expression, String path) {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> WritableDataTypeValidator.validateExpression(expression, "expression"));
    Assertions.assertEquals(
        "\"" + path + "\" field is required and cannot be null", exception.getMessage());
  }

  @ParameterizedTest
  @MethodSource("invalidPartitions")
  public void testRejectNullPartitionArrays(Partition partition, String path) {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> WritableDataTypeValidator.validatePartition(partition, "partition"));
    Assertions.assertEquals(
        "\"" + path + "\" field is required and cannot be null", exception.getMessage());
  }

  @Test
  public void testRejectUnsupportedPartition() {
    Partition partition =
        new Partition() {
          @Override
          public String name() {
            return "p";
          }

          @Override
          public Map<String, String> properties() {
            return Map.of();
          }
        };
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> WritableDataTypeValidator.validatePartition(partition, "partition"));
    Assertions.assertEquals(
        "\"partition\" must be an IdentityPartition, ListPartition or RangePartition",
        exception.getMessage());
  }

  @Test
  public void testPreserveAbsentExpressionsAndEmptyArrays() {
    Assertions.assertDoesNotThrow(
        () -> WritableDataTypeValidator.validateExpression(null, "defaultValue"));
    Assertions.assertDoesNotThrow(
        () -> WritableDataTypeValidator.validateExpression(Literals.NULL, "defaultValue"));
    Assertions.assertDoesNotThrow(
        () ->
            WritableDataTypeValidator.validateExpression(FunctionExpression.of("f"), "expression"));
    Assertions.assertDoesNotThrow(
        () ->
            WritableDataTypeValidator.validateExpression(
                Transforms.list(new String[] {"c"}), "partitioning[0]"));
    Assertions.assertDoesNotThrow(
        () ->
            WritableDataTypeValidator.validatePartition(
                Partitions.list("p", new Literal<?>[0][], null), "partition"));
  }

  private static Stream<Arguments> invalidExpressions() {
    return Stream.of(
        Arguments.of(FunctionExpression.of("f", (Expression[]) null), "expression.funcArgs"),
        Arguments.of(Transforms.apply("f", null), "expression.funcArgs"),
        Arguments.of((Expression) () -> null, "expression.children"),
        Arguments.of(Transforms.list(new String[][] {{"c"}}, null), "expression.assignments"),
        Arguments.of(Transforms.range(new String[] {"c"}, null), "expression.assignments"),
        Arguments.of(
            Transforms.list(new String[][] {{"c"}}, new ListPartition[] {null}),
            "expression.assignments[0]"));
  }

  private static Stream<Arguments> invalidPartitions() {
    return Stream.of(
        Arguments.of(null, "partition"),
        Arguments.of(
            Partitions.identity("p", new String[][] {{"c"}}, null, null), "partition.values"),
        Arguments.of(Partitions.list("p", null, null), "partition.lists"),
        Arguments.of(
            Partitions.list("p", new Literal<?>[][] {{Literals.integerLiteral(1)}, null}, null),
            "partition.lists[1]"));
  }
}
