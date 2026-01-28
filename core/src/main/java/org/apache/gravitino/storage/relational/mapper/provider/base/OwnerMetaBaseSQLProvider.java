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

import static org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper.OWNER_TABLE_NAME;

import java.util.Optional;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.ibatis.annotations.Param;

public class OwnerMetaBaseSQLProvider {
  public String selectUserOwnerMetaByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return "SELECT ut.user_id as userId,"
        + " ut.user_name as userName,"
        + " ut.metalake_id as metalakeId,"
        + " ut.audit_info as auditInfo,"
        + " ut.current_version as currentVersion,"
        + " ut.last_version as lastVersion,"
        + " ut.deleted_at as deletedAt"
        + " FROM "
        + OWNER_TABLE_NAME
        + " ot JOIN "
        + UserMetaMapper.USER_TABLE_NAME
        + " ut ON ut.user_id = ot.owner_id"
        + " WHERE ot.metadata_object_id = #{metadataObjectId} AND"
        + " ot.metadata_object_type = #{metadataObjectType} AND"
        + " ot.owner_type = 'USER' AND"
        + " ot.deleted_at = 0 AND ut.deleted_at = 0";
  }

  public String selectGroupOwnerMetaByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return "SELECT gt.group_id as groupId,"
        + " gt.group_name as groupName,"
        + " gt.metalake_id as metalakeId,"
        + " gt.audit_info as auditInfo,"
        + " gt.current_version as currentVersion,"
        + " gt.last_version as lastVersion,"
        + " gt.deleted_at as deletedAt"
        + " FROM "
        + OWNER_TABLE_NAME
        + " ot JOIN "
        + GroupMetaMapper.GROUP_TABLE_NAME
        + " gt ON gt.group_id = ot.owner_id"
        + " WHERE ot.metadata_object_id = #{metadataObjectId} AND"
        + " ot.metadata_object_type = #{metadataObjectType} AND"
        + " ot.owner_type = 'GROUP' AND"
        + " ot.deleted_at = 0 AND gt.deleted_at = 0";
  }

  public String insertOwnerRel(@Param("ownerRelPO") OwnerRelPO ownerRelPO) {
    return "INSERT INTO "
        + OWNER_TABLE_NAME
        + " (metalake_id, metadata_object_id, metadata_object_type, owner_id, owner_type,"
        + " audit_info, current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{ownerRelPO.metalakeId},"
        + " #{ownerRelPO.metadataObjectId},"
        + " #{ownerRelPO.metadataObjectType},"
        + " #{ownerRelPO.ownerId},"
        + " #{ownerRelPO.ownerType},"
        + " #{ownerRelPO.auditInfo},"
        + " #{ownerRelPO.currentVersion},"
        + " #{ownerRelPO.lastVersion},"
        + " #{ownerRelPO.deletedAt}"
        + ")";
  }

  public String softDeleteOwnerRelByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return "UPDATE "
        + OWNER_TABLE_NAME
        + softDeleteSQL()
        + " WHERE metadata_object_id = #{metadataObjectId} AND metadata_object_type = #{metadataObjectType} AND deleted_at = 0";
  }

  public String softDeleteOwnerRelByOwnerIdAndType(
      @Param("ownerId") Long ownerId, @Param("ownerType") String ownerType) {
    return "UPDATE "
        + OWNER_TABLE_NAME
        + softDeleteSQL()
        + " WHERE owner_id = #{ownerId} AND owner_type = #{ownerType} AND deleted_at = 0";
  }

  public String softDeleteOwnerRelByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + OWNER_TABLE_NAME
        + softDeleteSQL()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteOwnerRelByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + OWNER_TABLE_NAME
        + " ot"
        + softDeleteSQL(Optional.of("ot"))
        + " WHERE ot.deleted_at = 0 AND EXISTS ("
        + CatalogSchemaExistsSQLHelper.generateExistsSQL(
            "ot", "metadata_object_id", "metadata_object_type", "catalog_id", "catalogId", true)
        + ")";
  }

  public String softDeleteOwnerRelBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + OWNER_TABLE_NAME
        + " ot"
        + softDeleteSQL(Optional.of("ot"))
        + " WHERE ot.deleted_at = 0 AND EXISTS ("
        + CatalogSchemaExistsSQLHelper.generateExistsSQL(
            "ot", "metadata_object_id", "metadata_object_type", "schema_id", "schemaId", false)
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

  public String deleteOwnerMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + OWNER_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
