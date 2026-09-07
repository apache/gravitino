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

class TestViewMetaBaseSQLProvider {

  private static final ViewMetaBaseSQLProvider PROVIDER = new ViewMetaBaseSQLProvider();

  @Test
  void testUpdateUsesVersionCas() {
    String sql = PROVIDER.updateViewMeta(null, null);
    Assertions.assertTrue(sql.contains("metalake_id = #{newViewMeta.metalakeId}"));
    Assertions.assertTrue(sql.contains("catalog_id = #{newViewMeta.catalogId}"));
    String whereClause = sql.substring(sql.indexOf(" WHERE"));

    Assertions.assertEquals(
        " WHERE view_id = #{oldViewMeta.viewId} "
            + " AND current_version = #{oldViewMeta.currentVersion} "
            + " AND deleted_at = 0",
        whereClause);
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql = PROVIDER.softDeleteViewMetasByViewId(null, null);

    Assertions.assertTrue(sql.contains("AND current_version = #{currentVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }

  @Test
  void testConflictReadLocksActiveRow() {
    String sql = PROVIDER.selectViewMetaByIdForUpdate(null);

    Assertions.assertTrue(sql.contains("view_name as viewName"));
    Assertions.assertTrue(sql.contains("current_version as currentVersion"));
    Assertions.assertTrue(sql.contains("WHERE view_id = #{viewId} AND deleted_at = 0"));
    Assertions.assertTrue(sql.endsWith("FOR UPDATE"));
  }

  @Test
  void testNormalReadRequiresCurrentVersionRow() {
    String sql = PROVIDER.selectViewMetaBySchemaIdAndName(null, null);

    Assertions.assertTrue(sql.contains("vm INNER JOIN view_version_info vi"));
    Assertions.assertTrue(sql.endsWith("AND vm.deleted_at = 0 AND vi.deleted_at = 0"));
  }

  @Test
  void testOverwriteLookupLocksOnlyViewRoot() {
    String sql = PROVIDER.selectViewMetaBySchemaIdAndNameForUpdate(null, null);

    Assertions.assertFalse(sql.contains("view_version_info"));
    Assertions.assertTrue(sql.contains("current_version as currentVersion"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0 FOR UPDATE"));
  }
}
