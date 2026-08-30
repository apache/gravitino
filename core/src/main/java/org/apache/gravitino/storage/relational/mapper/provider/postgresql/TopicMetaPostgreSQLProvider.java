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

import static org.apache.gravitino.storage.relational.mapper.TopicMetaMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.provider.base.TopicMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.ibatis.annotations.Param;

public class TopicMetaPostgreSQLProvider extends TopicMetaBaseSQLProvider {
  @Override
  public String softDeleteTopicMetasByTopicId(Long topicId, Long currentVersion) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE topic_id = #{topicId}"
        + " AND current_version = #{currentVersion} AND deleted_at = 0";
  }

  @Override
  public String softDeleteTopicMetasByCatalogId(Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteTopicMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteTopicMetasBySchemaIds(List<Long> schemaIds) {
    return "<script>"
        + "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + ") AND deleted_at = 0"
        + "</script>";
  }

  @Override
  public String insertTopicMetaOnDuplicateKeyUpdate(TopicPO topicPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (topic_id, topic_name, metalake_id, catalog_id, schema_id,"
        + " comment, properties, audit_info, current_version, last_version,"
        + " deleted_at)"
        + " VALUES ("
        + " #{topicMeta.topicId},"
        + " #{topicMeta.topicName},"
        + " #{topicMeta.metalakeId},"
        + " #{topicMeta.catalogId},"
        + " #{topicMeta.schemaId},"
        + " #{topicMeta.comment},"
        + " #{topicMeta.properties},"
        + " #{topicMeta.auditInfo},"
        + " #{topicMeta.currentVersion},"
        + " #{topicMeta.lastVersion},"
        + " #{topicMeta.deletedAt}"
        + " )"
        // Overwrite is selected by name, and an import can carry a different ID for a topic that
        // already has an active registration. Target the natural key so PostgreSQL preserves the
        // stored topic ID, matching MySQL and H2.
        + " ON CONFLICT (schema_id, topic_name, deleted_at) DO UPDATE SET"
        + " topic_name = #{topicMeta.topicName},"
        + " metalake_id = #{topicMeta.metalakeId},"
        + " catalog_id = #{topicMeta.catalogId},"
        + " schema_id = #{topicMeta.schemaId},"
        + " comment = #{topicMeta.comment},"
        + " properties = #{topicMeta.properties},"
        + " audit_info = #{topicMeta.auditInfo},"
        // PostgreSQL evaluates both assignments against the stored row. Qualifying the columns
        // distinguishes them from the row that caused the conflict, and taking the larger marker
        // prevents an inconsistent legacy row from moving either version backwards.
        + " current_version = "
        + "GREATEST("
        + TABLE_NAME
        + ".current_version, "
        + TABLE_NAME
        + ".last_version) + 1,"
        + " last_version = "
        + "GREATEST("
        + TABLE_NAME
        + ".current_version, "
        + TABLE_NAME
        + ".last_version) + 1,"
        + " deleted_at = #{topicMeta.deletedAt}";
  }

  @Override
  public String deleteTopicMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE topic_id IN (SELECT topic_id FROM "
        + TABLE_NAME
        + " WHERE deleted_at != 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
