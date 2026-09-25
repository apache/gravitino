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

import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.DIMENSIONS;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.DISTANCE_FUNCTION;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.LEGACY_TYPE;

import java.util.Map;
import org.apache.gravitino.rel.indexes.Index;
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

  @Test
  public void testLegacyAnnoyAndUSearchIndexTypesArePreserved() {
    // Legacy types must stay distinguishable from the current vector_similarity HNSW index.
    Assertions.assertEquals(
        Index.IndexType.DATA_SKIPPING_ANNOY, operations.getClickHouseIndexType("annoy"));
    Assertions.assertEquals(
        Index.IndexType.DATA_SKIPPING_ANNOY, operations.getClickHouseIndexType("annoy('L2', 128)"));
    Assertions.assertEquals(
        Index.IndexType.DATA_SKIPPING_USEARCH, operations.getClickHouseIndexType("usearch"));
    Assertions.assertEquals(
        Index.IndexType.DATA_SKIPPING_USEARCH,
        operations.getClickHouseIndexType("usearch('cosineDistance', 512)"));
    Assertions.assertNotEquals(
        Index.IndexType.DATA_SKIPPING_ANNOY, operations.getClickHouseIndexType("ngrambf_v1"));
    Assertions.assertNotEquals(
        Index.IndexType.DATA_SKIPPING_USEARCH, operations.getClickHouseIndexType("bloom_filter"));
  }

  @Test
  public void testParseLegacyVectorIndexProperties() {
    Map<String, String> annoy =
        ClickHouseTableOperations.parseLegacyVectorIndexProperties(
            Index.IndexType.DATA_SKIPPING_ANNOY, "annoy('L2Distance', 128)");
    Assertions.assertEquals("annoy('L2Distance', 128)", annoy.get(LEGACY_TYPE));
    Assertions.assertEquals("L2Distance", annoy.get(DISTANCE_FUNCTION));
    Assertions.assertEquals("128", annoy.get(DIMENSIONS));

    Map<String, String> usearch =
        ClickHouseTableOperations.parseLegacyVectorIndexProperties(
            Index.IndexType.DATA_SKIPPING_USEARCH, "usearch('cosineDistance', 512)");
    Assertions.assertEquals("usearch('cosineDistance', 512)", usearch.get(LEGACY_TYPE));
    Assertions.assertEquals("cosineDistance", usearch.get(DISTANCE_FUNCTION));
    Assertions.assertEquals("512", usearch.get(DIMENSIONS));

    // A bare legacy type still preserves the type name and never loses the metadata.
    Assertions.assertEquals(
        "usearch",
        ClickHouseTableOperations.parseLegacyVectorIndexProperties(
                Index.IndexType.DATA_SKIPPING_USEARCH, "usearch")
            .get(LEGACY_TYPE));

    // Non-legacy index types are not affected.
    Assertions.assertTrue(
        ClickHouseTableOperations.parseLegacyVectorIndexProperties(
                Index.IndexType.DATA_SKIPPING_MINMAX, "minmax")
            .isEmpty());
  }
}
