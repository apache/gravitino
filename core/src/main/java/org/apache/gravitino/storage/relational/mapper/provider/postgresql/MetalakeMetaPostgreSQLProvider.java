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

import static org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.MetalakeMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.ibatis.annotations.Param;

public class MetalakeMetaPostgreSQLProvider extends MetalakeMetaBaseSQLProvider {
  @Override
  public String softDeleteMetalakeMetaByMetalakeId(Long metalakeId, Long currentVersion) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        // OCC: version-checked delete (see the base provider for rationale).
        + " WHERE metalake_id = #{metalakeId} AND current_version = #{currentVersion}"
        + " AND deleted_at = 0";
  }

  @Override
  public String insertMetalakeMetaOnDuplicateKeyUpdate(MetalakePO metalakePO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (metalake_id, metalake_name, metalake_comment, properties, audit_info,"
        + " schema_version, current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{metalakeMeta.metalakeId},"
        + " #{metalakeMeta.metalakeName},"
        + " #{metalakeMeta.metalakeComment},"
        + " #{metalakeMeta.properties},"
        + " #{metalakeMeta.auditInfo},"
        + " #{metalakeMeta.schemaVersion},"
        + " #{metalakeMeta.currentVersion},"
        + " #{metalakeMeta.lastVersion},"
        + " #{metalakeMeta.deletedAt}"
        + " )"
        + " ON CONFLICT(metalake_id) DO UPDATE SET"
        + " metalake_name = #{metalakeMeta.metalakeName},"
        + " metalake_comment = #{metalakeMeta.metalakeComment},"
        + " properties = #{metalakeMeta.properties},"
        + " audit_info = #{metalakeMeta.auditInfo},"
        + " schema_version = #{metalakeMeta.schemaVersion},"
        + " current_version = #{metalakeMeta.currentVersion},"
        + " last_version = #{metalakeMeta.lastVersion},"
        + " deleted_at = #{metalakeMeta.deletedAt}";
  }

  @Override
  public String updateMetalakeMeta(
      @Param("newMetalakeMeta") MetalakePO newMetalakePO,
      @Param("oldMetalakeMeta") MetalakePO oldMetalakePO) {
    return "UPDATE "
        + TABLE_NAME
        + " SET metalake_name = #{newMetalakeMeta.metalakeName},"
        + " metalake_comment = #{newMetalakeMeta.metalakeComment},"
        + " properties = #{newMetalakeMeta.properties},"
        + " audit_info = #{newMetalakeMeta.auditInfo},"
        + " schema_version = #{newMetalakeMeta.schemaVersion},"
        + " current_version = #{newMetalakeMeta.currentVersion},"
        + " last_version = #{newMetalakeMeta.lastVersion}"
        // OCC: compare-and-set on the version alone (current_version is monotonic on update).
        + " WHERE metalake_id = #{oldMetalakeMeta.metalakeId}"
        + " AND current_version = #{oldMetalakeMeta.currentVersion}"
        + " AND deleted_at = 0";
  }

  @Override
  public String deleteMetalakeMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE metalake_id IN (SELECT metalake_id FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
