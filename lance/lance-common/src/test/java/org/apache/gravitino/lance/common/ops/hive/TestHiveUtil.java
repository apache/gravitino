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
package org.apache.gravitino.lance.common.ops.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.InvalidInputException;

/** Unit tests for {@link HiveUtil}. */
class TestHiveUtil {

  @Test
  void testCreateLanceTableParamsAddsTypeAndManagedBy() {
    Map<String, String> params =
        HiveUtil.createLanceTableParams(ImmutableMap.of("custom", "value"));
    assertEquals("lance", params.get("table_type"));
    assertEquals("storage", params.get("managed_by"));
    assertEquals("value", params.get("custom"));
  }

  @Test
  void testCreateLanceTableParamsWithNull() {
    Map<String, String> params = HiveUtil.createLanceTableParams(null);
    assertEquals("lance", params.get("table_type"));
    assertEquals("storage", params.get("managed_by"));
  }

  @Test
  void testValidateLanceTablePasses() {
    Table table = new Table();
    table.setDbName("db");
    table.setTableName("t");
    table.setParameters(ImmutableMap.of("table_type", "lance"));
    HiveUtil.validateLanceTable(table);
  }

  @Test
  void testValidateLanceTableThrowsForNonLance() {
    Table table = new Table();
    table.setDbName("db");
    table.setTableName("t");
    table.setParameters(ImmutableMap.of("table_type", "iceberg"));
    assertThrows(InvalidInputException.class, () -> HiveUtil.validateLanceTable(table));
  }

  @Test
  void testValidateLanceTableThrowsForNullParams() {
    Table table = new Table();
    table.setDbName("db");
    table.setTableName("t");
    assertThrows(InvalidInputException.class, () -> HiveUtil.validateLanceTable(table));
  }

  @Test
  void testMakeQualifiedStripsTrailingSlash() {
    assertEquals("s3://bucket/wh", HiveUtil.makeQualified("s3://bucket/wh/"));
    assertEquals("s3://bucket/wh", HiveUtil.makeQualified("s3://bucket/wh"));
  }
}
