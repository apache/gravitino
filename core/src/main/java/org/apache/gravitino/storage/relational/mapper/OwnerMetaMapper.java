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

import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface OwnerMetaMapper {

  String OWNER_TABLE_NAME = "owner_meta";

  @Select(
      "SELECT metalake_id as metalakeId,"
          + " owner_id as ownerId,"
          + " owner_type as ownerType,"
          + " entity_id as entityId,"
          + " entity_type as entityType,"
          + " audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + OWNER_TABLE_NAME
          + " WHERE entity_id = #{entityId}"
          + " AND deleted_at = 0")
  OwnerRelPO selectOwnerMetaByEntityIdAndType(@Param("entityId") Long entityId);

  @Insert(
      "INSERT INTO "
          + OWNER_TABLE_NAME
          + "(metalake_id, entity_id, entity_type, owner_id, owner_type,"
          + " audit_info, current_version, last_version, deleted_at)"
          + " VALUES ("
          + " #{ownerRelPO.metalakeId},"
          + " #{ownerRelPO.entityId},"
          + " #{ownerRelPO.entityType},"
          + " #{ownerRelPO.ownerId},"
          + " #{ownerRelPO.ownerType},"
          + " #{ownerRelPO.auditInfo},"
          + " #{ownerRelPO.currentVersion},"
          + " #{ownerRelPO.lastVersion},"
          + " #{ownerRelPO.deletedAt}"
          + ")")
  void insertOwnerEntityRel(@Param("ownerRelPO") OwnerRelPO ownerRelPO);

  @Update(
      "UPDATE "
          + OWNER_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE entity_id = #{entityId} AND deleted_at = 0")
  void softDeleteOwnerRelByEntityId(@Param("entityId") Long entityId);

  @Update(
      "UPDATE  "
          + OWNER_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at =0")
  void softDeleteOwnerRelByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Update(
      "UPDATE  "
          + OWNER_TABLE_NAME
          + " ot SET ot.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE EXISTS ("
          + " SELECT ct.catalog_id FROM "
          + CatalogMetaMapper.TABLE_NAME
          + " ct where ct.catalog_id = #{catalogId}  AND ct.deleted_at = 0 AND ot.deleted_at = 0 AND ct.catalog_id = ot.entity_id"
          + " UNION "
          + " SELECT st.catalog_id FROM "
          + SchemaMetaMapper.TABLE_NAME
          + " st where st.catalog_id = #{catalogId} AND st.deleted_at = 0 AND ot.deleted_at = 0 AND st.schema_id = ot.entity_id"
          + " UNION "
          + " SELECT tt.catalog_id FROM "
          + TopicMetaMapper.TABLE_NAME
          + " tt where tt.catalog_id = #{catalogId} AND tt.deleted_at = 0 AND ot.deleted_at = 0 AND tt.topic_id = ot.entity_id"
          + " UNION "
          + " SELECT tat.catalog_id FROM "
          + TableMetaMapper.TABLE_NAME
          + " tat where tat.catalog_id = #{catalogId} AND tat.deleted_at = 0 AND ot.deleted_at = 0 AND tat.table_id = ot.entity_id"
          + " UNION "
          + " SELECT ft.catalog_id FROM "
          + FilesetMetaMapper.META_TABLE_NAME
          + " ft where ft.catalog_id = #{catalogId} AND ft.deleted_at = 0 AND ot.deleted_at = 0 AND ft.fileset_id = ot.entity_id"
          + ")")
  void softDeleteOwnerRelByCatalogId(@Param("catalogId") Long catalogId);

  @Update(
      "UPDATE  "
          + OWNER_TABLE_NAME
          + " ot SET ot.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE EXISTS ("
          + " SELECT st.schema_id FROM "
          + SchemaMetaMapper.TABLE_NAME
          + " st where st.schema_id = #{schemaId} AND st.deleted_at = 0 AND ot.deleted_at = 0 AND st.schema_id = ot.entity_id"
          + " UNION "
          + " SELECT tt.schema_id FROM "
          + TopicMetaMapper.TABLE_NAME
          + " tt where tt.schema_id = #{schemaId} AND tt.deleted_at = 0 AND ot.deleted_at = 0 AND tt.topic_id = ot.entity_id"
          + " UNION "
          + " SELECT tat.schema_id FROM "
          + TableMetaMapper.TABLE_NAME
          + " tat where tat.schema_id = #{schemaId} AND tat.deleted_at = 0 AND ot.deleted_at = 0 AND tat.table_id = ot.entity_id"
          + " UNION "
          + " SELECT ft.schema_id FROM "
          + FilesetMetaMapper.META_TABLE_NAME
          + " ft where ft.schema_id = #{schemaId} AND ft.deleted_at = 0 AND ot.deleted_at = 0 AND ft.fileset_id = ot.entity_id"
          + ")")
  void sotDeleteOwnerRelBySchemaId(@Param("schemaId") Long schemaId);

  @Delete(
      "DELETE FROM "
          + OWNER_TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteOwnerMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
