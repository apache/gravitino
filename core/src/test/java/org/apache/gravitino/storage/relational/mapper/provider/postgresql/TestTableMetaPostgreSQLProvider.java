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

import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestTableMetaPostgreSQLProvider {

  @Test
  void testOverwriteAdvancesStoredVersion() {
    String updateClause = conflictClause();

    // An overwrite must never write the initial version back, or a stale writer could still pass
    // its own version check afterwards.
    Assertions.assertTrue(
        updateClause.contains(
            "current_version = " + TableMetaMapper.TABLE_NAME + ".current_version + 1"),
        () -> "current_version must advance in: " + updateClause);
    Assertions.assertTrue(
        updateClause.contains(
            "last_version = " + TableMetaMapper.TABLE_NAME + ".current_version + 1"),
        () -> "last_version must advance in: " + updateClause);

    // PostgreSQL rejects a bare column name on this side of ON CONFLICT, because it could mean
    // either the stored row or the rejected one. Both assignments must name the table.
    Assertions.assertFalse(
        updateClause.matches(".*[^.\\w]current_version\\s*\\+.*"),
        () -> "Found an unqualified current_version reference in: " + updateClause);
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql = new TableMetaPostgreSQLProvider().softDeleteTableMetasByTableId(null, null);

    Assertions.assertTrue(sql.contains("AND current_version = #{currentVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }

  private String conflictClause() {
    String sql = new TableMetaPostgreSQLProvider().insertTableMetaOnDuplicateKeyUpdate(null);
    return sql.substring(sql.indexOf(" ON CONFLICT"));
  }
}
