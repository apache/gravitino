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

import java.util.Collections;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSchemaMetaPostgreSQLProvider {

  private static final SchemaMetaPostgreSQLProvider PROVIDER = new SchemaMetaPostgreSQLProvider();

  @Test
  void testOverwriteInsertQualifiesVersionColumns() {
    assertQualifiedAndAdvanced(conflictClause(PROVIDER.insertSchemaMetaOnDuplicateKeyUpdate(null)));
  }

  @Test
  void testBatchOverwriteInsertQualifiesVersionColumns() {
    assertQualifiedAndAdvanced(
        conflictClause(
            PROVIDER.batchInsertSchemaMetaOnDuplicateKeyUpdate(Collections.emptyList())));
  }

  private void assertQualifiedAndAdvanced(String conflictClause) {
    // PostgreSQL rejects a bare column name on this side of ON CONFLICT, because it could mean
    // either the stored row or the rejected one. Both assignments must name the table.
    Assertions.assertFalse(
        conflictClause.matches(".*[^.\\w]current_version\\s*\\+.*"),
        () -> "Found an unqualified current_version reference in: " + conflictClause);

    // An overwrite must never write the initial version back, or a stale writer could still pass
    // its own version check afterwards.
    Assertions.assertTrue(
        conflictClause.contains(
            "current_version = " + SchemaMetaMapper.TABLE_NAME + ".current_version + 1"),
        () -> "current_version must advance in: " + conflictClause);
    Assertions.assertTrue(
        conflictClause.contains(
            "last_version = " + SchemaMetaMapper.TABLE_NAME + ".current_version + 1"),
        () -> "last_version must advance in: " + conflictClause);
  }

  private String conflictClause(String sql) {
    return sql.substring(sql.indexOf("ON CONFLICT"));
  }
}
