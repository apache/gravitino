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

class TestTableMetaBaseSQLProvider {

  private static final TableMetaBaseSQLProvider PROVIDER = new TableMetaBaseSQLProvider();

  @Test
  void testOverwriteAdvancesStoredVersion() {
    String sql = PROVIDER.insertTableMetaOnDuplicateKeyUpdate(null);
    String updateClause = sql.substring(sql.indexOf(" ON DUPLICATE KEY UPDATE"));

    Assertions.assertTrue(updateClause.contains("last_version = current_version + 1"));
    Assertions.assertTrue(updateClause.contains("current_version = current_version + 1"));
    Assertions.assertFalse(updateClause.contains("current_version = #{tableMeta.currentVersion}"));
    Assertions.assertFalse(updateClause.contains("last_version = #{tableMeta.lastVersion}"));
  }

  @Test
  void testUpdateUsesOnlyIdVersionAndActiveStateForCas() {
    String sql = PROVIDER.updateTableMeta(null, null, null);
    String whereClause = sql.substring(sql.indexOf(" WHERE"));

    Assertions.assertEquals(
        " WHERE table_id = #{oldTableMeta.tableId}"
            + " AND current_version = #{oldTableMeta.currentVersion}"
            + " AND deleted_at = 0",
        whereClause);
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql = PROVIDER.softDeleteTableMetasByTableId(null, null);

    Assertions.assertTrue(sql.contains("AND current_version = #{currentVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }

  @Test
  void testConflictReadLocksActiveRow() {
    String sql = PROVIDER.selectTableMetaByIdForUpdate(null);

    Assertions.assertTrue(sql.contains("WHERE table_id = #{tableId} AND deleted_at = 0"));
    Assertions.assertTrue(sql.endsWith("FOR UPDATE"));
  }

  @Test
  void testAggregateReadRejectsDynamicRootSql() {
    TableMetaBaseSQLProvider dynamic =
        new TableMetaBaseSQLProvider() {
          @Override
          public String selectTableMetaBySchemaIdAndName(Long schemaId, String tableName) {
            return " <script>SELECT 1</script>";
          }

          @Override
          public String selectTableByFullQualifiedName(
              String metalake, String catalog, String schema, String table) {
            return "<script>SELECT 1</script>";
          }
        };
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> dynamic.selectTableWithColumnsBySchemaIdAndName(1L, "table"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> dynamic.selectTableWithColumnsByFullQualifiedName("ml", "cat", "schema", "table"));
  }

  @Test
  void testAggregateReadAcceptsStaticRootWithTrailingSemicolon() {
    String sql = PROVIDER.selectTableWithColumnsByFullQualifiedName("ml", "cat", "schema", "table");
    Assertions.assertTrue(sql.startsWith("SELECT t.*"));
    Assertions.assertFalse(sql.contains(";)"));
    Assertions.assertFalse(sql.contains("<script>"));
  }
}
