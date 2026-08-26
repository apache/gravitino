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

import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestModelMetaPostgreSQLProvider {

  @Test
  void testOverwriteAdvancesStoredVersion() {
    String sql = new ModelMetaPostgreSQLProvider().insertModelMetaOnDuplicateKeyUpdate(null);
    String insertClause = sql.substring(0, sql.indexOf(" ON CONFLICT"));
    String updateClause = conflictClause();

    Assertions.assertTrue(insertClause.contains("current_version, last_version"));
    Assertions.assertTrue(
        insertClause.contains("#{modelMeta.currentVersion}, #{modelMeta.lastVersion}"));
    Assertions.assertTrue(
        updateClause.contains(
            "current_version = " + ModelMetaMapper.TABLE_NAME + ".current_version + 1"));
    Assertions.assertTrue(
        updateClause.contains(
            "last_version = " + ModelMetaMapper.TABLE_NAME + ".current_version + 1"));
    Assertions.assertFalse(updateClause.matches(".*[^.\\w]current_version\\s*\\+.*"));
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql = new ModelMetaPostgreSQLProvider().softDeleteModelMetaByIdAndVersion(null, null);

    Assertions.assertTrue(sql.contains("AND current_version = #{currentVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }

  private String conflictClause() {
    String sql = new ModelMetaPostgreSQLProvider().insertModelMetaOnDuplicateKeyUpdate(null);
    return sql.substring(sql.indexOf(" ON CONFLICT"));
  }
}
