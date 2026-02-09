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
package org.apache.gravitino.catalog.hologres.operation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.junit.jupiter.api.Test;

/** Unit tests for partition-related methods in {@link HologresTableOperations}. */
public class TestHologresTableOperations {

  @Test
  void testAppendPartitioningSqlPhysicalPartition() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"ds"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendPartitioningSql(partitioning, false, sqlBuilder);
    String result = sqlBuilder.toString();
    assertTrue(result.contains("PARTITION BY LIST(\"ds\")"));
    // Should NOT contain LOGICAL keyword
    assertTrue(!result.contains("LOGICAL"));
  }

  @Test
  void testAppendPartitioningSqlLogicalPartitionSingleColumn() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"ds"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendPartitioningSql(partitioning, true, sqlBuilder);
    String result = sqlBuilder.toString();
    assertTrue(result.contains("LOGICAL PARTITION BY LIST(\"ds\")"));
  }

  @Test
  void testAppendPartitioningSqlLogicalPartitionTwoColumns() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"region"}, {"ds"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendPartitioningSql(partitioning, true, sqlBuilder);
    String result = sqlBuilder.toString();
    assertTrue(result.contains("LOGICAL PARTITION BY LIST(\"region\", \"ds\")"));
  }

  @Test
  void testAppendPartitioningSqlRejectsNonListTransform() {
    Transform[] partitioning = {Transforms.identity("col1")};
    StringBuilder sqlBuilder = new StringBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendPartitioningSql(partitioning, false, sqlBuilder));
  }

  @Test
  void testAppendPartitioningSqlPhysicalRejectsMultipleColumns() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"col1"}, {"col2"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    // Physical partition only supports 1 partition column
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendPartitioningSql(partitioning, false, sqlBuilder));
  }

  @Test
  void testAppendPartitioningSqlLogicalRejectsMoreThanTwoColumns() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"col1"}, {"col2"}, {"col3"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    // Logical partition supports at most 2 partition columns
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendPartitioningSql(partitioning, true, sqlBuilder));
  }

  @Test
  void testAppendIndexesSql() {
    // Verify that appendIndexesSql works correctly (existing functionality)
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendIndexesSql(
        new org.apache.gravitino.rel.indexes.Index[0], sqlBuilder);
    assertTrue(sqlBuilder.toString().isEmpty());
  }
}
