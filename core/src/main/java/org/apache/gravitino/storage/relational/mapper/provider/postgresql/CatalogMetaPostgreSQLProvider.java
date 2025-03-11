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

import static org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.CatalogMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.ibatis.annotations.Param;

public class CatalogMetaPostgreSQLProvider extends CatalogMetaBaseSQLProvider {
  @Override
  public String softDeleteCatalogMetasByCatalogId(Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteCatalogMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String deleteCatalogMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE catalog_id IN (SELECT catalog_id FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }

  @Override
  public String insertCatalogMetaOnDuplicateKeyUpdate(CatalogPO catalogPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + "(catalog_id, catalog_name, metalake_id,"
        + " type, provider, catalog_comment, properties, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES("
        + " #{catalogMeta.catalogId},"
        + " #{catalogMeta.catalogName},"
        + " #{catalogMeta.metalakeId},"
        + " #{catalogMeta.type},"
        + " #{catalogMeta.provider},"
        + " #{catalogMeta.catalogComment},"
        + " #{catalogMeta.properties},"
        + " #{catalogMeta.auditInfo},"
        + " #{catalogMeta.currentVersion},"
        + " #{catalogMeta.lastVersion},"
        + " #{catalogMeta.deletedAt}"
        + " )"
        + " ON CONFLICT(catalog_id) DO UPDATE SET"
        + " catalog_name = #{catalogMeta.catalogName},"
        + " metalake_id = #{catalogMeta.metalakeId},"
        + " type = #{catalogMeta.type},"
        + " provider = #{catalogMeta.provider},"
        + " catalog_comment = #{catalogMeta.catalogComment},"
        + " properties = #{catalogMeta.properties},"
        + " audit_info = #{catalogMeta.auditInfo},"
        + " current_version = #{catalogMeta.currentVersion},"
        + " last_version = #{catalogMeta.lastVersion},"
        + " deleted_at = #{catalogMeta.deletedAt}";
  }

  @Override
  public String updateCatalogMeta(
      @Param("newCatalogMeta") CatalogPO newCatalogPO,
      @Param("oldCatalogMeta") CatalogPO oldCatalogPO) {
    return "UPDATE "
        + TABLE_NAME
        + " SET catalog_name = #{newCatalogMeta.catalogName},"
        + " metalake_id = #{newCatalogMeta.metalakeId},"
        + " type = #{newCatalogMeta.type},"
        + " provider = #{newCatalogMeta.provider},"
        + " catalog_comment = #{newCatalogMeta.catalogComment},"
        + " properties = #{newCatalogMeta.properties},"
        + " audit_info = #{newCatalogMeta.auditInfo},"
        + " current_version = #{newCatalogMeta.currentVersion},"
        + " last_version = #{newCatalogMeta.lastVersion},"
        + " deleted_at = #{newCatalogMeta.deletedAt}"
        + " WHERE catalog_id = #{oldCatalogMeta.catalogId}"
        + " AND catalog_name = #{oldCatalogMeta.catalogName}"
        + " AND metalake_id = #{oldCatalogMeta.metalakeId}"
        + " AND type = #{oldCatalogMeta.type}"
        + " AND provider = #{oldCatalogMeta.provider}"
        + " AND (catalog_comment = #{oldCatalogMeta.catalogComment} "
        + "   OR (CAST(catalog_comment AS VARCHAR) IS NULL AND "
        + "   CAST(#{oldCatalogMeta.catalogComment} AS VARCHAR) IS NULL))"
        + " AND properties = #{oldCatalogMeta.properties}"
        + " AND audit_info = #{oldCatalogMeta.auditInfo}"
        + " AND current_version = #{oldCatalogMeta.currentVersion}"
        + " AND last_version = #{oldCatalogMeta.lastVersion}"
        + " AND deleted_at = 0";
  }
}
