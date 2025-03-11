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

import static org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.ibatis.annotations.Param;

public class CatalogMetaBaseSQLProvider {
  public String listCatalogPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "SELECT catalog_id as catalogId, catalog_name as catalogName,"
        + " metalake_id as metalakeId, type, provider,"
        + " catalog_comment as catalogComment, properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String listCatalogPOsByCatalogIds(@Param("catalogIds") List<Long> catalogIds) {
    return "<script>"
        + "SELECT catalog_id as catalogId, catalog_name as catalogName,"
        + " metalake_id as metalakeId, type, provider,"
        + " catalog_comment as catalogComment, properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE catalog_id in ("
        + "<foreach collection='catalogIds' item='catalogId' separator=','>"
        + "#{catalogId}"
        + "</foreach>"
        + ") "
        + " AND deleted_at = 0"
        + "</script>";
  }

  public String selectCatalogIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("catalogName") String name) {
    return "SELECT catalog_id as catalogId FROM "
        + TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND catalog_name = #{catalogName} AND deleted_at = 0";
  }

  public String selectCatalogMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("catalogName") String name) {
    return "SELECT catalog_id as catalogId, catalog_name as catalogName,"
        + " metalake_id as metalakeId, type, provider,"
        + " catalog_comment as catalogComment, properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND catalog_name = #{catalogName} AND deleted_at = 0";
  }

  public String selectCatalogMetaById(@Param("catalogId") Long catalogId) {
    return "SELECT catalog_id as catalogId, catalog_name as catalogName,"
        + " metalake_id as metalakeId, type, provider,"
        + " catalog_comment as catalogComment, properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String insertCatalogMeta(@Param("catalogMeta") CatalogPO catalogPO) {
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
        + " )";
  }

  public String insertCatalogMetaOnDuplicateKeyUpdate(@Param("catalogMeta") CatalogPO catalogPO) {
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
        + " ON DUPLICATE KEY UPDATE"
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
        + "   OR (catalog_comment IS NULL and #{oldCatalogMeta.catalogComment} IS NULL))"
        + " AND properties = #{oldCatalogMeta.properties}"
        + " AND audit_info = #{oldCatalogMeta.auditInfo}"
        + " AND current_version = #{oldCatalogMeta.currentVersion}"
        + " AND last_version = #{oldCatalogMeta.lastVersion}"
        + " AND deleted_at = 0";
  }

  public String softDeleteCatalogMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteCatalogMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String deleteCatalogMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
