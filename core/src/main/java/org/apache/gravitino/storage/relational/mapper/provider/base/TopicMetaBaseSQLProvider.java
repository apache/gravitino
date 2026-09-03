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

import static org.apache.gravitino.storage.relational.mapper.TopicMetaMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.ibatis.annotations.Param;

public class TopicMetaBaseSQLProvider {

  public String insertTopicMeta(@Param("topicMeta") TopicPO topicPO) {
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
        + " )";
  }

  public String insertTopicMetaOnDuplicateKeyUpdate(@Param("topicMeta") TopicPO topicPO) {
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
        + " ON DUPLICATE KEY UPDATE"
        + " topic_name = #{topicMeta.topicName},"
        + " metalake_id = #{topicMeta.metalakeId},"
        + " catalog_id = #{topicMeta.catalogId},"
        + " schema_id = #{topicMeta.schemaId},"
        + " comment = #{topicMeta.comment},"
        + " properties = #{topicMeta.properties},"
        + " audit_info = #{topicMeta.auditInfo},"
        + overwriteVersionAssignments()
        + " deleted_at = #{topicMeta.deletedAt}";
  }

  public String listTopicPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return "SELECT topic_id as topicId, topic_name as topicName, metalake_id as metalakeId,"
        + " catalog_id as catalogId, schema_id as schemaId,"
        + " comment as comment, properties as properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String listTopicPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName) {
    return """
        SELECT
            mm.metalake_id as metalakeId,
            sm.schema_id as schemaId,
            cm.catalog_id as catalogId,
            tm.topic_id as topicId,
            tm.topic_name as topicName,
            tm.comment as comment,
            tm.properties as properties,
            tm.audit_info as auditInfo,
            tm.current_version as currentVersion,
            tm.last_version as lastVersion,
            tm.deleted_at as deletedAt
        FROM
            %s mm
        INNER JOIN
            %s cm ON mm.metalake_id = cm.metalake_id
            AND cm.catalog_name = #{catalogName}
            AND cm.deleted_at = 0
        LEFT JOIN
            %s sm ON cm.catalog_id = sm.catalog_id
            AND sm.schema_name = #{schemaName}
            AND sm.deleted_at = 0
        LEFT JOIN
            %s tm ON sm.schema_id = tm.schema_id
            AND tm.deleted_at = 0
        WHERE
            mm.metalake_name = #{metalakeName}
            AND mm.deleted_at = 0;
            """
        .formatted(
            MetalakeMetaMapper.TABLE_NAME,
            CatalogMetaMapper.TABLE_NAME,
            SchemaMetaMapper.TABLE_NAME,
            TABLE_NAME);
  }

  public String listTopicPOsByTopicIds(@Param("topicIds") List<Long> topicIds) {
    return "<script>"
        + " SELECT topic_id as topicId, topic_name as topicName, metalake_id as metalakeId,"
        + " catalog_id as catalogId, schema_id as schemaId,"
        + " comment as comment, properties as properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE deleted_at = 0"
        + " AND topic_id IN ("
        + "<foreach collection='topicIds' item='topicId' separator=','>"
        + "#{topicId}"
        + "</foreach>"
        + ") "
        + "</script>";
  }

  public String selectTopicMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("topicName") String topicName) {
    return "SELECT topic_id as topicId, topic_name as topicName,"
        + " metalake_id as metalakeId, catalog_id as catalogId, schema_id as schemaId,"
        + " comment as comment, properties as properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND topic_name = #{topicName} AND deleted_at = 0";
  }

  public String selectTopicByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("topicName") String topicName) {
    return """
        SELECT
            mm.metalake_id as metalakeId,
            sm.schema_id as schemaId,
            cm.catalog_id as catalogId,
            tm.topic_id as topicId,
            tm.topic_name as topicName,
            tm.comment as comment,
            tm.properties as properties,
            tm.audit_info as auditInfo,
            tm.current_version as currentVersion,
            tm.last_version as lastVersion,
            tm.deleted_at as deletedAt
        FROM
            %s mm
        INNER JOIN
            %s cm ON mm.metalake_id = cm.metalake_id
            AND cm.catalog_name = #{catalogName}
            AND cm.deleted_at = 0
        LEFT JOIN
            %s sm ON cm.catalog_id = sm.catalog_id
            AND sm.schema_name = #{schemaName}
            AND sm.deleted_at = 0
        LEFT JOIN
            %s tm ON sm.schema_id = tm.schema_id
            AND tm.topic_name = #{topicName}
            AND tm.deleted_at = 0
        WHERE
            mm.metalake_name = #{metalakeName}
            AND mm.deleted_at = 0;
            """
        .formatted(
            MetalakeMetaMapper.TABLE_NAME,
            CatalogMetaMapper.TABLE_NAME,
            SchemaMetaMapper.TABLE_NAME,
            TABLE_NAME);
  }

  public String selectTopicMetaById(@Param("topicId") Long topicId) {
    return "SELECT topic_id as topicId, topic_name as topicName,"
        + " metalake_id as metalakeId, catalog_id as catalogId, schema_id as schemaId,"
        + " comment as comment, properties as properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE topic_id = #{topicId} AND deleted_at = 0";
  }

  /**
   * Returns an active topic metadata row and locks it for the current transaction.
   *
   * <p>The stable ID lets a failed CAS distinguish a newer topic from one that was deleted,
   * renamed, or moved while the caller was writing.
   *
   * @param topicId the topic ID
   * @return the locking select SQL
   */
  public String selectTopicMetaByIdForUpdate(@Param("topicId") Long topicId) {
    return selectTopicMetaById(topicId) + " FOR UPDATE";
  }

  /**
   * Returns SQL that updates a topic only while its OCC version is unchanged.
   *
   * <p>The version is the concurrency token. Comparing payload columns would miss a writer that
   * changes a value and then changes it back before this update runs.
   *
   * @param newTopicPO the new topic values
   * @param oldTopicPO the topic values and version observed by the caller
   * @return the version-checked update SQL
   */
  public String updateTopicMeta(
      @Param("newTopicMeta") TopicPO newTopicPO, @Param("oldTopicMeta") TopicPO oldTopicPO) {
    return "UPDATE "
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
        + " AND current_version = #{oldTopicMeta.currentVersion}"
        + " AND deleted_at = 0";
  }

  public String selectTopicIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("topicName") String name) {
    return "SELECT topic_id as topicId FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND topic_name = #{topicName}"
        + " AND deleted_at = 0";
  }

  /**
   * Returns SQL that deletes only the topic version observed by the caller.
   *
   * @param topicId the topic ID
   * @param currentVersion the version observed by the caller
   * @return the version-checked delete SQL
   */
  public String softDeleteTopicMetasByTopicId(
      @Param("topicId") Long topicId, @Param("currentVersion") Long currentVersion) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE topic_id = #{topicId}"
        + " AND current_version = #{currentVersion} AND deleted_at = 0";
  }

  public String softDeleteTopicMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteTopicMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteTopicMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>"
        + "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + ") AND deleted_at = 0"
        + "</script>";
  }

  public String deleteTopicMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at != 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  public String batchSelectTopicByIdentifier(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("topicNames") List<String> topicNames) {
    return "<script>"
        + "SELECT tm.topic_id as topicId, tm.topic_name as topicName,"
        + " tm.metalake_id as metalakeId, tm.catalog_id as catalogId, tm.schema_id as schemaId,"
        + " tm.comment as comment, tm.properties as properties, tm.audit_info as auditInfo,"
        + " tm.current_version as currentVersion, tm.last_version as lastVersion,"
        + " tm.deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " tm"
        + " JOIN "
        + SchemaMetaMapper.TABLE_NAME
        + " sm ON tm.schema_id = sm.schema_id"
        + " JOIN "
        + CatalogMetaMapper.TABLE_NAME
        + " cm ON sm.catalog_id = cm.catalog_id"
        + " JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON cm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName}"
        + " AND cm.catalog_name = #{catalogName}"
        + " AND sm.schema_name = #{schemaName}"
        + " AND tm.topic_name IN ("
        + "<foreach collection='topicNames' item='topicName' separator=','>"
        + "#{topicName}"
        + "</foreach>"
        + " )"
        + " AND tm.deleted_at = 0 AND sm.deleted_at = 0 AND cm.deleted_at = 0 AND mm.deleted_at = 0"
        + "</script>";
  }

  /**
   * Returns MySQL assignments that advance an overwritten topic beyond both stored version markers.
   *
   * <p>MySQL evaluates assignments from left to right. Updating {@code current_version} first lets
   * {@code last_version} copy the same newly computed value without evaluating the maximum again
   * against a partially updated row.
   *
   * @return the overwrite version assignments
   */
  protected String overwriteVersionAssignments() {
    return " current_version = GREATEST(current_version, last_version) + 1,"
        + " last_version = current_version,";
  }
}
