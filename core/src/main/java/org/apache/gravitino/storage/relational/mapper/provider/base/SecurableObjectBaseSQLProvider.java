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
import java.util.Optional;
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
        + softDeleteSQL()
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
        + softDeleteSQL()
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  public String softDeleteSecurableObjectsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " ob"
        + softDeleteSQL(Optional.of("ob"))
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
        + softDeleteSQL()
        + " WHERE metadata_object_id = #{metadataObjectId} AND deleted_at = 0"
        + " AND type = #{metadataObjectType}";
  }

  public String softDeleteObjectRelsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " sect"
        + softDeleteSQL(Optional.of("sect"))
        + " WHERE sect.deleted_at = 0 AND EXISTS ("
        + CatalogSchemaExistsSQLHelper.generateExistsSQL(
            "sect", "metadata_object_id", "type", "catalog_id", "catalogId", true)
        + ")";
  }

  public String softDeleteObjectRelsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " sect"
        + softDeleteSQL(Optional.of("sect"))
        + " WHERE sect.deleted_at = 0 AND EXISTS ("
        + CatalogSchemaExistsSQLHelper.generateExistsSQL(
            "sect", "metadata_object_id", "type", "schema_id", "schemaId", false)
        + ")";
  }

  protected String softDeleteSQL() {
    return softDeleteSQL(Optional.empty());
  }

  protected String softDeleteSQL(Optional<String> tableAlias) {
    String prefix = tableAlias.map(alias -> alias + ".").orElse("");
    return " SET "
        + prefix
        + "deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000 ";
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
