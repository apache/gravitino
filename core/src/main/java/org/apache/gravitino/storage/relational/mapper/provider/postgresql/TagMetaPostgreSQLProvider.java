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

import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.TagMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.ibatis.annotations.Param;

public class TagMetaPostgreSQLProvider extends TagMetaBaseSQLProvider {
  @Override
  public String softDeleteTagMetaByMetalakeAndTagName(String metalakeName, String tagName) {
    return "UPDATE "
        + TAG_TABLE_NAME
        + " tm SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE tm.metalake_id IN ("
        + " SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND tm.tag_name = #{tagName} AND tm.deleted_at = 0";
  }

  @Override
  public String softDeleteTagMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + TAG_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String insertTagMetaOnDuplicateKeyUpdate(TagPO tagPO) {
    return "INSERT INTO "
        + TAG_TABLE_NAME
        + "(tag_id, tag_name,"
        + " metalake_id, tag_comment, properties, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES("
        + " #{tagMeta.tagId},"
        + " #{tagMeta.tagName},"
        + " #{tagMeta.metalakeId},"
        + " #{tagMeta.comment},"
        + " #{tagMeta.properties},"
        + " #{tagMeta.auditInfo},"
        + " #{tagMeta.currentVersion},"
        + " #{tagMeta.lastVersion},"
        + " #{tagMeta.deletedAt}"
        + " )"
        + " ON CONFLICT(tag_id) DO UPDATE SET"
        + " tag_name = #{tagMeta.tagName},"
        + " metalake_id = #{tagMeta.metalakeId},"
        + " tag_comment = #{tagMeta.comment},"
        + " properties = #{tagMeta.properties},"
        + " audit_info = #{tagMeta.auditInfo},"
        + " current_version = #{tagMeta.currentVersion},"
        + " last_version = #{tagMeta.lastVersion},"
        + " deleted_at = #{tagMeta.deletedAt}";
  }

  @Override
  public String updateTagMeta(
      @Param("newTagMeta") TagPO newTagPO, @Param("oldTagMeta") TagPO oldTagPO) {
    return "UPDATE "
        + TAG_TABLE_NAME
        + " SET tag_name = #{newTagMeta.tagName},"
        + " tag_comment = #{newTagMeta.comment},"
        + " properties = #{newTagMeta.properties},"
        + " audit_info = #{newTagMeta.auditInfo},"
        + " current_version = #{newTagMeta.currentVersion},"
        + " last_version = #{newTagMeta.lastVersion},"
        + " deleted_at = #{newTagMeta.deletedAt}"
        + " WHERE tag_id = #{oldTagMeta.tagId}"
        + " AND metalake_id = #{oldTagMeta.metalakeId}"
        + " AND tag_name = #{oldTagMeta.tagName}"
        + " AND (tag_comment = #{oldTagMeta.comment} "
        + "   OR (CAST(tag_comment AS VARCHAR) IS NULL AND CAST(#{oldTagMeta.comment} AS VARCHAR) IS NULL))"
        + " AND properties = #{oldTagMeta.properties}"
        + " AND audit_info = #{oldTagMeta.auditInfo}"
        + " AND current_version = #{oldTagMeta.currentVersion}"
        + " AND last_version = #{oldTagMeta.lastVersion}"
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
