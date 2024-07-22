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

import java.util.List;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface TopicMetaMapper {
  String TABLE_NAME = "topic_meta";

  @Insert(
      "INSERT INTO "
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
          + " )")
  void insertTopicMeta(@Param("topicMeta") TopicPO topicPO);

  @Insert(
      "INSERT INTO "
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
          + " ON DUPLICATE KEY UPDATE"
          + " topic_name = #{topicMeta.topicName},"
          + " metalake_id = #{topicMeta.metalakeId},"
          + " catalog_id = #{topicMeta.catalogId},"
          + " schema_id = #{topicMeta.schemaId},"
          + " comment = #{topicMeta.comment},"
          + " properties = #{topicMeta.properties},"
          + " audit_info = #{topicMeta.auditInfo},"
          + " current_version = #{topicMeta.currentVersion},"
          + " last_version = #{topicMeta.lastVersion},"
          + " deleted_at = #{topicMeta.deletedAt}")
  void insertTopicMetaOnDuplicateKeyUpdate(@Param("topicMeta") TopicPO topicPO);

  @Select(
      "SELECT topic_id as topicId, topic_name as topicName, metalake_id as metalakeId,"
          + " catalog_id as catalogId, schema_id as schemaId,"
          + " comment as comment, properties as properties, audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE schema_id = #{schemaId} AND deleted_at = 0")
  List<TopicPO> listTopicPOsBySchemaId(@Param("schemaId") Long schemaId);

  @Select(
      "SELECT topic_id as topicId, topic_name as topicName,"
          + " metalake_id as metalakeId, catalog_id as catalogId, schema_id as schemaId,"
          + " comment as comment, properties as properties, audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE schema_id = #{schemaId} AND topic_name = #{topicName} AND deleted_at = 0")
  TopicPO selectTopicMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("topicName") String topicName);

  @Select(
      "SELECT topic_id as topicId, topic_name as topicName,"
          + " metalake_id as metalakeId, catalog_id as catalogId, schema_id as schemaId,"
          + " comment as comment, properties as properties, audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE topic_id = #{topicId} AND deleted_at = 0")
  TopicPO selectTopicMetaById(@Param("topicId") Long topicId);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET topic_name = #{newTopicMeta.topicName},"
          + " metalake_id = #{newTopicMeta.metalakeId},"
          + " catalog_id = #{newTopicMeta.catalogId},"
          + " schema_id = #{newTopicMeta.schemaId},"
          + " comment = #{newTopicMeta.comment},"
          + " properties = #{newTopicMeta.properties},"
          + " audit_info = #{newTopicMeta.auditInfo},"
          + " current_version = #{newTopicMeta.currentVersion},"
          + " last_version = #{newTopicMeta.lastVersion},"
          + " deleted_at = #{newTopicMeta.deletedAt}"
          + " WHERE topic_id = #{oldTopicMeta.topicId}"
          + " AND topic_name = #{oldTopicMeta.topicName}"
          + " AND metalake_id = #{oldTopicMeta.metalakeId}"
          + " AND catalog_id = #{oldTopicMeta.catalogId}"
          + " AND schema_id = #{oldTopicMeta.schemaId}"
          + " AND comment = #{oldTopicMeta.comment}"
          + " AND properties = #{oldTopicMeta.properties}"
          + " AND audit_info = #{oldTopicMeta.auditInfo}"
          + " AND current_version = #{oldTopicMeta.currentVersion}"
          + " AND last_version = #{oldTopicMeta.lastVersion}"
          + " AND deleted_at = 0")
  Integer updateTopicMeta(
      @Param("newTopicMeta") TopicPO newTopicPO, @Param("oldTopicMeta") TopicPO oldTopicPO);

  @Select(
      "SELECT topic_id as topicId FROM "
          + TABLE_NAME
          + " WHERE schema_id = #{schemaId} AND topic_name = #{topicName}"
          + " AND deleted_at = 0")
  Long selectTopicIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("topicName") String name);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE topic_id = #{topicId} AND deleted_at = 0")
  Integer softDeleteTopicMetasByTopicId(@Param("topicId") Long topicId);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0")
  Integer softDeleteTopicMetasByCatalogId(@Param("catalogId") Long catalogId);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  Integer softDeleteTopicMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE schema_id = #{schemaId} AND deleted_at = 0")
  Integer softDeleteTopicMetasBySchemaId(@Param("schemaId") Long schemaId);

  @Delete(
      "DELETE FROM "
          + TABLE_NAME
          + " WHERE deleted_at != 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteTopicMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
