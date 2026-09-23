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

import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestFilesetMetaBaseSQLProvider {

  private static final FilesetMetaBaseSQLProvider PROVIDER = new FilesetMetaBaseSQLProvider();

  @Test
  void testOverwriteAdvancesStoredVersion() {
    String sql = PROVIDER.insertFilesetMetaOnDuplicateKeyUpdate(null);
    String updateClause = sql.substring(sql.indexOf(" ON DUPLICATE KEY UPDATE"));

    // The overwrite advances the OCC token only. The history version is the join key into
    // fileset_version_info and this statement writes no snapshot to move it to.
    Assertions.assertTrue(updateClause.contains("occ_version = occ_version + 1"));
    Assertions.assertFalse(updateClause.contains("current_version ="));
    Assertions.assertFalse(updateClause.contains("last_version ="));
    Assertions.assertFalse(updateClause.contains("occ_version = #{filesetMeta.occVersion}"));
  }

  @Test
  void testUpdateUsesVersionCasAndRejectsAnOccupiedSnapshotVersion() {
    String sql = PROVIDER.updateFilesetMeta(null, null);
    String whereClause = sql.substring(sql.indexOf(" WHERE"));

    Assertions.assertEquals(
        " WHERE fileset_id = #{oldFilesetMeta.filesetId}"
            + " AND occ_version = #{oldFilesetMeta.occVersion}"
            + " AND deleted_at = 0"
            + " AND NOT EXISTS (SELECT 1 FROM fileset_version_info fv"
            + " WHERE fv.fileset_id = #{oldFilesetMeta.filesetId}"
            + " AND fv.version >= #{newFilesetMeta.currentVersion}"
            + " AND fv.deleted_at = 0)",
        whereClause);
  }

  @Test
  void testUpdateDropsTheSnapshotCheckWhenNoVersionIsAllocated() {
    // An alter that changes nothing the version table stores keeps current_version where it is,
    // and the snapshot it points at is supposed to exist, so the check would reject every such
    // alter.
    FilesetPO unchanged = Mockito.mock(FilesetPO.class);
    Mockito.when(unchanged.getCurrentVersion()).thenReturn(3L);
    FilesetPO stored = Mockito.mock(FilesetPO.class);
    Mockito.when(stored.getCurrentVersion()).thenReturn(3L);

    String sql = PROVIDER.updateFilesetMeta(unchanged, stored);

    Assertions.assertFalse(sql.contains("NOT EXISTS"));
    Assertions.assertTrue(
        sql.endsWith(
            " WHERE fileset_id = #{oldFilesetMeta.filesetId}"
                + " AND occ_version = #{oldFilesetMeta.occVersion}"
                + " AND deleted_at = 0"));
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql = PROVIDER.softDeleteFilesetMetasByFilesetId(null, null);

    Assertions.assertTrue(sql.contains("AND occ_version = #{occVersion}"));
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
