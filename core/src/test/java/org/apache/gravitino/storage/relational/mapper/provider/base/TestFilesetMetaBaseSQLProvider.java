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

class TestFilesetMetaBaseSQLProvider {

  private static final FilesetMetaBaseSQLProvider PROVIDER = new FilesetMetaBaseSQLProvider();

  @Test
  void testOverwriteAdvancesStoredVersion() {
    String sql = PROVIDER.insertFilesetMetaOnDuplicateKeyUpdate(null);
    String updateClause = sql.substring(sql.indexOf(" ON DUPLICATE KEY UPDATE"));

    Assertions.assertTrue(updateClause.contains("last_version = current_version + 1"));
    Assertions.assertTrue(updateClause.contains("current_version = current_version + 1"));
    Assertions.assertTrue(
        updateClause.indexOf("last_version =") < updateClause.indexOf("current_version ="));
    Assertions.assertFalse(
        updateClause.contains("current_version = #{filesetMeta.currentVersion}"));
    Assertions.assertFalse(updateClause.contains("last_version = #{filesetMeta.lastVersion}"));
  }

  @Test
  void testUpdateUsesVersionCasAndRejectsAnOccupiedSnapshotVersion() {
    String sql = PROVIDER.updateFilesetMeta(null, null);
    String whereClause = sql.substring(sql.indexOf(" WHERE"));

    Assertions.assertEquals(
        " WHERE fileset_id = #{oldFilesetMeta.filesetId}"
            + " AND current_version = #{oldFilesetMeta.currentVersion}"
            + " AND deleted_at = 0"
            + " AND NOT EXISTS (SELECT 1 FROM fileset_version_info fv"
            + " WHERE fv.fileset_id = #{oldFilesetMeta.filesetId}"
            + " AND fv.version >= #{newFilesetMeta.currentVersion}"
            + " AND fv.deleted_at = 0)",
        whereClause);
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql = PROVIDER.softDeleteFilesetMetasByFilesetId(null, null);

    Assertions.assertTrue(sql.contains("AND current_version = #{currentVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }

  @Test
  void testOverwriteReadUsesNaturalKeyAndMetadataOnly() {
    String sql = PROVIDER.selectFilesetMetaBySchemaIdAndNameForUpdate(null, null);

    Assertions.assertTrue(
        sql.contains(
            "WHERE schema_id = #{schemaId} AND fileset_name = #{filesetName}"
                + " AND deleted_at = 0"));
    Assertions.assertFalse(sql.contains("fileset_version_info"));
    Assertions.assertTrue(sql.endsWith("FOR UPDATE"));
  }
}
