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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestFunctionMetaPostgreSQLProvider {

  @Test
  void testOverwriteAdvancesStoredVersion() {
    String sql = new FunctionMetaPostgreSQLProvider().insertFunctionMetaOnDuplicateKeyUpdate(null);
    String conflictClause = sql.substring(sql.indexOf(" ON CONFLICT"));

    Assertions.assertTrue(
        conflictClause.contains(
            "function_current_version = "
                + FunctionMetaMapper.TABLE_NAME
                + ".function_current_version + 1"));
    Assertions.assertTrue(
        conflictClause.contains(
            "function_latest_version = "
                + FunctionMetaMapper.TABLE_NAME
                + ".function_current_version + 1"));
    Assertions.assertFalse(conflictClause.matches(".*[^.\\w]function_current_version\\s*\\+.*"));
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql =
        new FunctionMetaPostgreSQLProvider().softDeleteFunctionMetaByFunctionId(null, null);

    Assertions.assertTrue(sql.contains("AND function_current_version = #{currentVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }

  @Test
  void testNormalReadRequiresCurrentVersionRow() {
    String sql =
        new FunctionMetaPostgreSQLProvider().selectFunctionMetaBySchemaIdAndName(null, null);

    Assertions.assertTrue(sql.contains("fm INNER JOIN function_version_info vi"));
    Assertions.assertTrue(sql.endsWith("AND fm.deleted_at = 0 AND vi.deleted_at = 0"));
  }
}
