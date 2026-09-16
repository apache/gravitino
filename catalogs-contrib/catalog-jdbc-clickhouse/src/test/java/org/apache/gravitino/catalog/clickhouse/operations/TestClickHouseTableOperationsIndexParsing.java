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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestClickHouseTableOperationsIndexParsing {

  private final ClickHouseTableOperations operations = new ClickHouseTableOperations();

  @Test
  public void testParseSimpleAndTupleIndexExpressions() {
    String[][] single = operations.parseIndexFields("col_1");
    Assertions.assertArrayEquals(new String[][] {{"col_1"}}, single);

    String[][] quoted = operations.parseIndexFields("`quoted_col`");
    Assertions.assertArrayEquals(new String[][] {{"quoted_col"}}, quoted);

    String[][] tuple = operations.parseIndexFields("tuple(`a`, b)");
    Assertions.assertArrayEquals(new String[][] {{"a"}, {"b"}}, tuple);
  }

  @Test
  public void testRejectFunctionWrappedExpression() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> operations.parseIndexFields("lower(name)"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.parseIndexFields("bloom_filter(cityHash64(user_id))"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.parseIndexFields("minmax(lower(`tenant_id`))"));
  }

  @Test
  public void testParseEmptyIndexExpression() {
    Assertions.assertEquals(0, operations.parseIndexFields("  ").length);
    Assertions.assertEquals(0, operations.parseIndexFields("tuple()").length);
  }

  @Test
  public void testUnsupportedExpression() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> operations.parseIndexFields("cityHash64(id) % 16"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.parseIndexFields("tuple(lower(name), id)"));
  }
}
