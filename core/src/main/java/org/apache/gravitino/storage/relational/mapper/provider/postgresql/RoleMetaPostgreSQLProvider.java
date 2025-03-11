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

import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.RoleMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.ibatis.annotations.Param;

public class RoleMetaPostgreSQLProvider extends RoleMetaBaseSQLProvider {
  @Override
  public String softDeleteRoleMetaByRoleId(Long roleId) {
    return "UPDATE "
        + ROLE_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteRoleMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + ROLE_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String insertRoleMetaOnDuplicateKeyUpdate(RolePO rolePO) {
    return "INSERT INTO "
        + ROLE_TABLE_NAME
        + "(role_id, role_name,"
        + " metalake_id, properties,"
        + " audit_info, current_version, last_version, deleted_at)"
        + " VALUES("
        + " #{roleMeta.roleId},"
        + " #{roleMeta.roleName},"
        + " #{roleMeta.metalakeId},"
        + " #{roleMeta.properties},"
        + " #{roleMeta.auditInfo},"
        + " #{roleMeta.currentVersion},"
        + " #{roleMeta.lastVersion},"
        + " #{roleMeta.deletedAt}"
        + " ) ON CONFLICT (role_id) DO UPDATE SET"
        + " role_name = #{roleMeta.roleName},"
        + " metalake_id = #{roleMeta.metalakeId},"
        + " properties = #{roleMeta.properties},"
        + " audit_info = #{roleMeta.auditInfo},"
        + " current_version = #{roleMeta.currentVersion},"
        + " last_version = #{roleMeta.lastVersion},"
        + " deleted_at = #{roleMeta.deletedAt}";
  }

  @Override
  public String deleteRoleMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + ROLE_TABLE_NAME
        + " WHERE role_id IN (SELECT role_id FROM "
        + ROLE_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
