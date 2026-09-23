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

import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestFilesetMetaPostgreSQLProvider {

  @Test
  void testOverwriteAdvancesStoredVersion() {
    String sql = new FilesetMetaPostgreSQLProvider().insertFilesetMetaOnDuplicateKeyUpdate(null);
    String conflictClause = sql.substring(sql.indexOf(" ON CONFLICT"));

    Assertions.assertTrue(
        conflictClause.startsWith(" ON CONFLICT(schema_id, fileset_name, deleted_at)"));
    // The overwrite advances the OCC token only. The history version is the join key into
    // fileset_version_info and this statement writes no snapshot to move it to.
    Assertions.assertTrue(
        conflictClause.contains(
            "occ_version = " + FilesetMetaMapper.META_TABLE_NAME + ".occ_version + 1"));
    Assertions.assertFalse(conflictClause.contains("current_version ="));
    Assertions.assertFalse(conflictClause.contains("last_version ="));
    Assertions.assertFalse(conflictClause.contains("#{filesetMeta.occVersion}"));
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql = new FilesetMetaPostgreSQLProvider().softDeleteFilesetMetasByFilesetId(null, null);

    Assertions.assertTrue(sql.contains("AND occ_version = #{occVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }
}
