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

import static org.apache.gravitino.storage.relational.mapper.TagMetaMapper.TAG_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.mapper.provider.base.TagMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.ibatis.annotations.Param;

public class TagMetaPostgreSQLProvider extends TagMetaBaseSQLProvider {
  @Override
  public String softDeleteTagMetaByIdAndVersion(Long tagId, Long currentVersion) {
    return "UPDATE "
        + TAG_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE tag_id = #{tagId} AND current_version = #{currentVersion}"
        + " AND deleted_at = 0";
  }

  @Override
  public String softDeleteTagMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + TAG_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String updateTagMeta(
      @Param("newTagMeta") TagPO newTagPO, @Param("oldTagMeta") TagPO oldTagPO) {
    return "UPDATE "
        + TAG_TABLE_NAME
        + " SET tag_name = #{newTagMeta.tagName},"
        + " tag_comment = #{newTagMeta.comment},"
        + " properties = #{newTagMeta.properties},"
        + " allowed_values = #{newTagMeta.allowedValues},"
        + " audit_info = #{newTagMeta.auditInfo},"
        + " current_version = #{newTagMeta.currentVersion},"
        + " last_version = #{newTagMeta.lastVersion},"
        + " deleted_at = #{newTagMeta.deletedAt}"
        + " WHERE tag_id = #{oldTagMeta.tagId}"
        + " AND current_version = #{oldTagMeta.currentVersion}"
        + " AND deleted_at = 0";
  }

  @Override
  public String deleteTagMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TAG_TABLE_NAME
        + " WHERE tag_id IN (SELECT tag_id FROM "
        + TAG_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
