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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestFunctionMetaBaseSQLProvider {

  private static final FunctionMetaBaseSQLProvider PROVIDER = new FunctionMetaBaseSQLProvider();

  @Test
  void testOverwriteAdvancesStoredVersion() {
    String sql = PROVIDER.insertFunctionMetaOnDuplicateKeyUpdate(null);
    String updateClause = sql.substring(sql.indexOf(" ON DUPLICATE KEY UPDATE"));

    Assertions.assertTrue(
        updateClause.contains("function_latest_version = function_current_version + 1"));
    Assertions.assertTrue(
        updateClause.contains("function_current_version = function_current_version + 1"));
    Assertions.assertFalse(
        updateClause.contains("function_current_version = #{functionMeta.functionCurrentVersion}"));
    Assertions.assertFalse(
        updateClause.contains("function_latest_version = #{functionMeta.functionLatestVersion}"));
  }

  @Test
  void testUpdateUsesOnlyIdVersionAndActiveStateForCas() {
    String sql = PROVIDER.updateFunctionMeta(null, null);
    String whereClause = sql.substring(sql.indexOf(" WHERE"));

    Assertions.assertEquals(
        " WHERE function_id = #{oldFunctionMeta.functionId}"
            + " AND function_current_version = #{oldFunctionMeta.functionCurrentVersion}"
            + " AND deleted_at = 0",
        whereClause);
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql = PROVIDER.softDeleteFunctionMetaByFunctionId(null, null);

    Assertions.assertTrue(sql.contains("AND function_current_version = #{currentVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }

  @Test
  void testConflictReadLocksActiveRow() {
    String sql = PROVIDER.selectFunctionMetaByIdForUpdate(null);

    Assertions.assertTrue(sql.contains("function_name as functionName"));
    Assertions.assertTrue(sql.contains("function_current_version as functionCurrentVersion"));
    Assertions.assertTrue(sql.contains("WHERE function_id = #{functionId} AND deleted_at = 0"));
    Assertions.assertTrue(sql.endsWith("FOR UPDATE"));
  }
}
