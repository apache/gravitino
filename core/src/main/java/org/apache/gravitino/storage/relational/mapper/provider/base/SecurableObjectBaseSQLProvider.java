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

import static org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper.ROLE_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper.SECURABLE_OBJECT_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.ibatis.annotations.Param;

public class SecurableObjectBaseSQLProvider {

  public String batchInsertSecurableObjects(
      @Param("securableObjects") List<SecurableObjectPO> securableObjectPOs) {
    return "<script>"
        + "INSERT INTO "
        + SECURABLE_OBJECT_TABLE_NAME
        + " (role_id, metadata_object_id, type, privilege_names, privilege_conditions,"
        + " current_version, last_version, deleted_at)"
        + " VALUES "
        + "<foreach collection='securableObjects' item='item' separator=','>"
        + "(#{item.roleId},"
        + " #{item.metadataObjectId},"
        + " #{item.type},"
        + " #{item.privilegeNames},"
        + " #{item.privilegeConditions},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})"
        + "</foreach>"
        + "</script>";
  }

  public String batchSoftDeleteSecurableObjects(
      @Param("securableObjects") List<SecurableObjectPO> securableObjectPOs) {
    return "<script>"
        + "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE FALSE "
        + "<foreach collection='securableObjects' item='item' separator=' '>"
        + " OR (metadata_object_id = #{item.metadataObjectId} AND"
        + " role_id = #{item.roleId} AND deleted_at = 0)"
        + "</foreach>"
        + "</script>";
  }

  public String softDeleteSecurableObjectsByRoleId(@Param("roleId") Long roleId) {
    return "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  public String softDeleteSecurableObjectsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " ob SET ob.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE exists (SELECT * FROM "
        + ROLE_TABLE_NAME
        + " ro WHERE ro.metalake_id = #{metalakeId} AND ro.role_id = ob.role_id"
        + " AND ro.deleted_at = 0) AND ob.deleted_at = 0";
  }

  public String softDeleteObjectRelsByMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metadata_object_id = #{metadataObjectId} AND deleted_at = 0"
        + " AND type = #{metadataObjectType}";
  }

  public String softDeleteObjectRelsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " sect SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE sect.deleted_at = 0 AND EXISTS ("
        + " SELECT ct.catalog_id FROM "
        + CatalogMetaMapper.TABLE_NAME
        + " ct WHERE ct.catalog_id = #{catalogId} AND"
        + " ct.catalog_id = sect.metadata_object_id AND sect.type = 'CATALOG'"
        + " UNION"
        + " SELECT st.catalog_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.catalog_id = #{catalogId} AND"
        + " st.schema_id = sect.metadata_object_id AND sect.type = 'SCHEMA'"
        + " UNION"
        + " SELECT tt.catalog_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.catalog_id = #{catalogId} AND"
        + " tt.topic_id = sect.metadata_object_id AND sect.type = 'TOPIC'"
        + " UNION"
        + " SELECT tat.catalog_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.catalog_id = #{catalogId} AND"
        + " tat.table_id = sect.metadata_object_id AND sect.type = 'TABLE'"
        + " UNION"
        + " SELECT ft.catalog_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.catalog_id = #{catalogId} AND"
        + " ft.fileset_id = sect.metadata_object_id AND sect.type = 'FILESET'"
        + " UNION"
        + " SELECT mt.catalog_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.catalog_id = #{catalogId} AND"
        + " mt.model_id = sect.metadata_object_id AND sect.type = 'MODEL'"
        + ")";
  }

  public String softDeleteObjectRelsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " sect SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE sect.deleted_at = 0 AND EXISTS ("
        + " SELECT st.schema_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.schema_id = #{schemaId}"
        + " AND st.schema_id = sect.metadata_object_id AND sect.type = 'SCHEMA'"
        + " UNION"
        + " SELECT tt.schema_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.schema_id = #{schemaId} AND"
        + " tt.topic_id = sect.metadata_object_id AND sect.type = 'TOPIC'"
        + " UNION"
        + " SELECT tat.schema_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.schema_id = #{schemaId} AND"
        + " tat.table_id = sect.metadata_object_id AND sect.type = 'TABLE'"
        + " UNION"
        + " SELECT ft.schema_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.schema_id = #{schemaId} AND"
        + " ft.fileset_id = sect.metadata_object_id AND sect.type = 'FILESET'"
        + " UNION"
        + " SELECT mt.schema_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.schema_id = #{schemaId} AND"
        + " mt.model_id = sect.metadata_object_id AND sect.type = 'MODEL'"
        + ")";
  }

  public String listSecurableObjectsByRoleId(@Param("roleId") Long roleId) {
    return "SELECT role_id as roleId, metadata_object_id as metadataObjectId,"
        + " type as type, privilege_names as privilegeNames,"
        + " privilege_conditions as privilegeConditions, current_version as currentVersion,"
        + " last_version as lastVersion, deleted_at as deletedAt"
        + " FROM "
        + SECURABLE_OBJECT_TABLE_NAME
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  public String deleteSecurableObjectsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + SECURABLE_OBJECT_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
