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

import org.apache.gravitino.storage.relational.mapper.provider.base.TopicMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.ibatis.annotations.Param;

public class TopicMetaPostgreSQLProvider extends TopicMetaBaseSQLProvider {

  @Override
  public String softDeleteTopicMetasByTopicId(Long topicId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE topic_id = #{topicId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteTopicMetasByCatalogId(Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteTopicMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteTopicMetasBySchemaId(Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  @Override
  public String insertTopicMetaOnDuplicateKeyUpdate(TopicPO topicPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + "(topic_id, topic_name, metalake_id, catalog_id, schema_id,"
        + " comment, properties, audit_info, current_version, last_version,"
        + " deleted_at)"
        + " VALUES("
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
        + " ON CONFLICT (topic_id) DO UPDATE SET"
        + " topic_name = #{topicMeta.topicName},"
        + " metalake_id = #{topicMeta.metalakeId},"
        + " catalog_id = #{topicMeta.catalogId},"
        + " schema_id = #{topicMeta.schemaId},"
        + " comment = #{topicMeta.comment},"
        + " properties = #{topicMeta.properties},"
        + " audit_info = #{topicMeta.auditInfo},"
        + " current_version = #{topicMeta.currentVersion},"
        + " last_version = #{topicMeta.lastVersion},"
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
