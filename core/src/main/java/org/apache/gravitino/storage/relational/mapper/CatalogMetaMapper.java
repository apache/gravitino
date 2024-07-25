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

package org.apache.gravitino.storage.relational.mapper;

import java.util.List;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * A MyBatis Mapper for catalog meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface CatalogMetaMapper {
  String TABLE_NAME = "catalog_meta";

  @Select(
      "SELECT catalog_id as catalogId, catalog_name as catalogName,"
          + " metalake_id as metalakeId, type, provider,"
          + " catalog_comment as catalogComment, properties, audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  List<CatalogPO> listCatalogPOsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Select(
      "SELECT catalog_id as catalogId FROM "
          + TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND catalog_name = #{catalogName} AND deleted_at = 0")
  Long selectCatalogIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("catalogName") String name);

  @Select(
      "SELECT catalog_id as catalogId, catalog_name as catalogName,"
          + " metalake_id as metalakeId, type, provider,"
          + " catalog_comment as catalogComment, properties, audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND catalog_name = #{catalogName} AND deleted_at = 0")
  CatalogPO selectCatalogMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("catalogName") String name);

  @Select(
      "SELECT catalog_id as catalogId, catalog_name as catalogName,"
          + " metalake_id as metalakeId, type, provider,"
          + " catalog_comment as catalogComment, properties, audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0")
  CatalogPO selectCatalogMetaById(@Param("catalogId") Long catalogId);

  @Insert(
      "INSERT INTO "
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
          + " )")
  void insertCatalogMeta(@Param("catalogMeta") CatalogPO catalogPO);

  @Insert(
      "INSERT INTO "
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
          + " deleted_at = #{catalogMeta.deletedAt}")
  void insertCatalogMetaOnDuplicateKeyUpdate(@Param("catalogMeta") CatalogPO catalogPO);

  @Update(
      "UPDATE "
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
          + " AND catalog_comment = #{oldCatalogMeta.catalogComment}"
          + " AND properties = #{oldCatalogMeta.properties}"
          + " AND audit_info = #{oldCatalogMeta.auditInfo}"
          + " AND current_version = #{oldCatalogMeta.currentVersion}"
          + " AND last_version = #{oldCatalogMeta.lastVersion}"
          + " AND deleted_at = 0")
  Integer updateCatalogMeta(
      @Param("newCatalogMeta") CatalogPO newCatalogPO,
      @Param("oldCatalogMeta") CatalogPO oldCatalogPO);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0")
  Integer softDeleteCatalogMetasByCatalogId(@Param("catalogId") Long catalogId);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  Integer softDeleteCatalogMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Delete(
      "DELETE FROM "
          + TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteCatalogMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
