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

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestModelMetaBaseSQLProvider {

  private static final ModelMetaBaseSQLProvider PROVIDER = new ModelMetaBaseSQLProvider();

  @Test
  void testInsertPersistsVersionColumns() {
    String sql = PROVIDER.insertModelMeta(null);

    Assertions.assertTrue(sql.contains("current_version, last_version"));
    Assertions.assertTrue(sql.contains("#{modelMeta.currentVersion}, #{modelMeta.lastVersion}"));
  }

  @Test
  void testOverwriteAdvancesStoredVersion() {
    String sql = PROVIDER.insertModelMetaOnDuplicateKeyUpdate(null);
    String insertClause = sql.substring(0, sql.indexOf(" ON DUPLICATE KEY UPDATE"));
    String updateClause = sql.substring(sql.indexOf(" ON DUPLICATE KEY UPDATE"));

    Assertions.assertTrue(insertClause.contains("current_version, last_version"));
    Assertions.assertTrue(
        insertClause.contains("#{modelMeta.currentVersion}, #{modelMeta.lastVersion}"));
    Assertions.assertTrue(updateClause.contains("last_version = current_version + 1"));
    Assertions.assertTrue(updateClause.contains("current_version = current_version + 1"));
    Assertions.assertTrue(
        updateClause.contains(
            "model_latest_version ="
                + " GREATEST(COALESCE(model_latest_version, 0),"
                + " #{modelMeta.modelLatestVersion})"));
    Assertions.assertFalse(updateClause.contains("current_version = #{modelMeta.currentVersion}"));
    Assertions.assertFalse(updateClause.contains("last_version = #{modelMeta.lastVersion}"));
  }

  @Test
  void testUpdateUsesOnlyIdVersionAndActiveStateForCas() {
    String sql = PROVIDER.updateModelMeta(null, null);
    String whereClause = sql.substring(sql.indexOf(" WHERE"));

    Assertions.assertEquals(
        " WHERE model_id = #{oldModelMeta.modelId}"
            + " AND current_version = #{oldModelMeta.currentVersion}"
            + " AND deleted_at = 0",
        whereClause);
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql = PROVIDER.softDeleteModelMetaByIdAndVersion(null, null);

    Assertions.assertTrue(sql.contains("AND current_version = #{currentVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }

  @Test
  void testConflictReadLocksActiveRow() {
    String sql = PROVIDER.selectModelMetaByModelIdForUpdate(null);

    Assertions.assertTrue(sql.contains("WHERE model_id = #{modelId} AND deleted_at = 0"));
    Assertions.assertTrue(sql.endsWith("FOR UPDATE"));
  }

  @Test
  void testAggregateVersionBumpIsGuardedOnIdentity() {
    String sql = PROVIDER.bumpModelVersion(null, null, null);

    Assertions.assertTrue(
        sql.contains("last_version = current_version + 1, current_version = current_version + 1"));
    // Guarded on the identity the caller resolved: a version another writer registered in the
    // meantime must not reject this write, while a dropped or renamed model must.
    Assertions.assertTrue(sql.contains("WHERE model_id = #{modelId}"));
    Assertions.assertTrue(sql.contains("AND schema_id = #{schemaId}"));
    Assertions.assertTrue(sql.contains("AND model_name = #{modelName}"));
    Assertions.assertFalse(sql.contains("AND current_version = #{currentVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }

  @Test
  void testEveryModelReadProjectsVersionColumns() {
    List<String> readSqls =
        List.of(
            PROVIDER.listModelPOsBySchemaId(null),
            PROVIDER.listModelPOsByFullQualifiedName(null, null, null),
            PROVIDER.listModelPOsByModelIds(null),
            PROVIDER.selectModelMetaBySchemaIdAndModelName(null, null),
            PROVIDER.selectModelByFullQualifiedName(null, null, null, null),
            PROVIDER.selectModelMetaByModelId(null),
            PROVIDER.selectModelMetaByModelIdForUpdate(null),
            PROVIDER.batchSelectModelByIdentifier(null, null, null, null));

    readSqls.forEach(
        sql -> {
          Assertions.assertTrue(sql.contains("currentVersion"), () -> "Missing OCC token: " + sql);
          Assertions.assertTrue(
              sql.contains("lastVersion"), () -> "Missing last OCC version: " + sql);
        });
  }
}
