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

import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermBindingMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetricMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermBindingPO;
import org.apache.ibatis.annotations.Param;

/** Portable SQL for glossary term bindings on H2, MySQL and PostgreSQL. */
public class GlossaryTermBindingBaseSQLProvider {
  private static final String TABLE = GlossaryTermBindingMapper.TABLE_NAME;
  private static final String SELECT_COLUMNS =
      "b.metalake_id AS metalakeId, b.term_id AS termId, b.target_type AS targetType,"
          + " b.target_id AS targetId, b.deleted_at AS deletedAt";

  /** Inserts a live binding row. */
  public String insert(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("targetType") String targetType,
      @Param("targetId") long targetId) {
    return "INSERT INTO "
        + TABLE
        + " (metalake_id, term_id, target_type, target_id, deleted_at)"
        + " VALUES (#{metalakeId}, #{termId}, #{targetType}, #{targetId}, 0)";
  }

  /** Retires one live binding row. */
  public String remove(
      @Param("termId") long termId,
      @Param("targetType") String targetType,
      @Param("targetId") long targetId,
      @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + TABLE
        + " SET deleted_at=#{deletedAt}"
        + " WHERE term_id=#{termId} AND target_type=#{targetType} AND target_id=#{targetId}"
        + " AND deleted_at=0";
  }

  /** Retires every live binding row of a term. */
  public String removeTerm(@Param("termId") long termId, @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + TABLE
        + " SET deleted_at=#{deletedAt} WHERE term_id=#{termId} AND deleted_at=0";
  }

  /** Retires every live binding row of a metalake. */
  public String dropMetalake(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + TABLE
        + " SET deleted_at=#{deletedAt} WHERE metalake_id=#{metalakeId} AND deleted_at=0";
  }

  /** Reads the latest tombstone time of a term. */
  public String lastDeletedAt(@Param("termId") long termId) {
    return "SELECT COALESCE(MAX(deleted_at), 0) FROM " + TABLE + " WHERE term_id=#{termId}";
  }

  /** Reads the latest tombstone time in a metalake. */
  public String lastDeletedAtInMetalake(@Param("metalakeId") long metalakeId) {
    return "SELECT COALESCE(MAX(deleted_at), 0) FROM " + TABLE + " WHERE metalake_id=#{metalakeId}";
  }

  /** Lists live term IDs bound to a target. */
  public String listTermIds(
      @Param("metalakeId") long metalakeId,
      @Param("targetType") String targetType,
      @Param("targetId") long targetId) {
    return "SELECT term_id FROM "
        + TABLE
        + " WHERE metalake_id=#{metalakeId} AND target_type=#{targetType}"
        + " AND target_id=#{targetId} AND deleted_at=0 ORDER BY term_id";
  }

  /** Lists live bindings for a nonempty batch of target IDs of one target type. */
  public String listByTargets(
      @Param("metalakeId") long metalakeId,
      @Param("targetType") String targetType,
      @Param("targetIds") Collection<Long> targetIds) {
    return "<script>SELECT "
        + SELECT_COLUMNS
        + " FROM "
        + TABLE
        + " b WHERE b.metalake_id=#{metalakeId} AND b.target_type=#{targetType} AND b.deleted_at=0"
        + " AND b.target_id IN "
        + "<foreach collection='targetIds' item='id' open='(' separator=',' close=')'>#{id}"
        + "</foreach> ORDER BY b.term_id, b.target_type, b.target_id</script>";
  }

  /** Lists live bindings of a term ordered by target type and ID. */
  public String listByTerm(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("targetTypes") @Nullable Collection<String> targetTypes) {
    StringBuilder sql =
        new StringBuilder("<script>SELECT ")
            .append(SELECT_COLUMNS)
            .append(" FROM ")
            .append(TABLE)
            .append(" b WHERE b.metalake_id=#{metalakeId} AND b.term_id=#{termId}")
            .append(" AND b.deleted_at=0");
    if (targetTypes != null && !targetTypes.isEmpty()) {
      sql.append(" AND b.target_type IN ")
          .append(
              "<foreach collection='targetTypes' item='t' open='(' separator=',' close=')'>#{t}</foreach>");
    }
    sql.append(" ORDER BY b.target_type, b.target_id</script>");
    return sql.toString();
  }

  /** Lists live bindings on a table and on any of its live columns. */
  public String listByTable(@Param("metalakeId") long metalakeId, @Param("tableId") long tableId) {
    return "SELECT DISTINCT "
        + SELECT_COLUMNS
        + " FROM "
        + TABLE
        + " b WHERE b.metalake_id=#{metalakeId} AND b.deleted_at=0 AND ("
        + "(b.target_type='TABLE' AND b.target_id=#{tableId}) OR "
        + "(b.target_type='COLUMN' AND b.target_id IN ("
        + "SELECT DISTINCT c.column_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " c WHERE c.table_id=#{tableId} AND c.deleted_at=0))"
        + ") ORDER BY b.term_id, b.target_type, b.target_id";
  }

  /** Lists live bindings on any live metric of a semantic model. */
  public String listBySemanticModel(
      @Param("metalakeId") long metalakeId, @Param("semanticModelId") long semanticModelId) {
    return "SELECT "
        + SELECT_COLUMNS
        + " FROM "
        + TABLE
        + " b WHERE b.metalake_id=#{metalakeId} AND b.deleted_at=0"
        + " AND b.target_type='METRIC' AND b.target_id IN ("
        + "SELECT m.metric_id FROM "
        + SemanticModelMetricMapper.TABLE_NAME
        + " m WHERE m.semantic_model_id=#{semanticModelId} AND m.deleted_at=0)"
        + " ORDER BY b.term_id, b.target_type, b.target_id";
  }

  /** Lists live bindings on schemas and all their live descendant objects and metrics. */
  public String listBySchemas(
      @Param("metalakeId") long metalakeId, @Param("schemaIds") Collection<Long> schemaIds) {
    String inSchemas =
        "<foreach collection='schemaIds' item='sid' open='(' separator=',' close=')'>#{sid}</foreach>";
    return "<script>SELECT DISTINCT "
        + SELECT_COLUMNS
        + " FROM "
        + TABLE
        + " b WHERE b.metalake_id=#{metalakeId} AND b.deleted_at=0 AND ("
        + "(b.target_type='SCHEMA' AND b.target_id IN "
        + inSchemas
        + ") OR "
        + "(b.target_type='TABLE' AND b.target_id IN (SELECT table_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " WHERE schema_id IN "
        + inSchemas
        + " AND deleted_at=0)) OR "
        + "(b.target_type='COLUMN' AND b.target_id IN (SELECT DISTINCT column_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " WHERE schema_id IN "
        + inSchemas
        + " AND deleted_at=0)) OR "
        + "(b.target_type='VIEW' AND b.target_id IN (SELECT view_id FROM "
        + ViewMetaMapper.TABLE_NAME
        + " WHERE schema_id IN "
        + inSchemas
        + " AND deleted_at=0)) OR "
        + "(b.target_type='FILESET' AND b.target_id IN (SELECT fileset_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " WHERE schema_id IN "
        + inSchemas
        + " AND deleted_at=0)) OR "
        + "(b.target_type='TOPIC' AND b.target_id IN (SELECT topic_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " WHERE schema_id IN "
        + inSchemas
        + " AND deleted_at=0)) OR "
        + "(b.target_type='MODEL' AND b.target_id IN (SELECT model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE schema_id IN "
        + inSchemas
        + " AND deleted_at=0)) OR "
        + "(b.target_type='METRIC' AND b.target_id IN (SELECT metric_id FROM "
        + SemanticModelMetricMapper.TABLE_NAME
        + " WHERE schema_id IN "
        + inSchemas
        + " AND deleted_at=0))"
        + ") ORDER BY b.term_id, b.target_type, b.target_id</script>";
  }

  /** Lists live bindings on a catalog and all its live descendant objects and metrics. */
  public String listByCatalog(
      @Param("metalakeId") long metalakeId, @Param("catalogId") long catalogId) {
    return "SELECT DISTINCT "
        + SELECT_COLUMNS
        + " FROM "
        + TABLE
        + " b WHERE b.metalake_id=#{metalakeId} AND b.deleted_at=0 AND ("
        + "(b.target_type='CATALOG' AND b.target_id=#{catalogId}) OR "
        + "(b.target_type='SCHEMA' AND b.target_id IN (SELECT schema_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " WHERE catalog_id=#{catalogId} AND deleted_at=0)) OR "
        + "(b.target_type='TABLE' AND b.target_id IN (SELECT table_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " WHERE catalog_id=#{catalogId} AND deleted_at=0)) OR "
        + "(b.target_type='COLUMN' AND b.target_id IN (SELECT DISTINCT column_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " WHERE catalog_id=#{catalogId} AND deleted_at=0)) OR "
        + "(b.target_type='VIEW' AND b.target_id IN (SELECT view_id FROM "
        + ViewMetaMapper.TABLE_NAME
        + " WHERE catalog_id=#{catalogId} AND deleted_at=0)) OR "
        + "(b.target_type='FILESET' AND b.target_id IN (SELECT fileset_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " WHERE catalog_id=#{catalogId} AND deleted_at=0)) OR "
        + "(b.target_type='TOPIC' AND b.target_id IN (SELECT topic_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " WHERE catalog_id=#{catalogId} AND deleted_at=0)) OR "
        + "(b.target_type='MODEL' AND b.target_id IN (SELECT model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE catalog_id=#{catalogId} AND deleted_at=0)) OR "
        + "(b.target_type='METRIC' AND b.target_id IN (SELECT metric_id FROM "
        + SemanticModelMetricMapper.TABLE_NAME
        + " WHERE catalog_id=#{catalogId} AND deleted_at=0))"
        + ") ORDER BY b.term_id, b.target_type, b.target_id";
  }

  /** Selects a bounded batch of retired rows for garbage collection. */
  public String expired(@Param("cutoff") long cutoff, @Param("limit") int limit) {
    return "SELECT metalake_id AS metalakeId, term_id AS termId, target_type AS targetType,"
        + " target_id AS targetId, deleted_at AS deletedAt FROM "
        + TABLE
        + " WHERE deleted_at>0 AND deleted_at<#{cutoff}"
        + " ORDER BY deleted_at, term_id, target_type, target_id LIMIT #{limit}";
  }

  /** Deletes only the selected retired generation. */
  public String purge(@Param("row") GlossaryTermBindingPO row) {
    return "DELETE FROM "
        + TABLE
        + " WHERE term_id=#{row.termId} AND target_type=#{row.targetType}"
        + " AND target_id=#{row.targetId} AND deleted_at=#{row.deletedAt} AND deleted_at>0";
  }

  /** Identifies roots with live bindings before expanding a multi-term preview. */
  public String listTermsWithBindings() {
    return """
        <script>
        SELECT t.term_id FROM %s t
        WHERE t.metalake_id=#{metalakeId} AND t.deleted_at=0 AND t.term_id IN
        <foreach collection="termIds" item="id" open="(" separator="," close=")">#{id}</foreach>
        AND EXISTS (SELECT 1 FROM %s b WHERE b.term_id=t.term_id
          AND b.metalake_id=t.metalake_id AND b.deleted_at=0)
        ORDER BY t.term_id
        </script>
        """
        .formatted(GlossaryTermMetaMapper.TABLE_NAME, TABLE);
  }

  /** Fetches binding candidates for a lookup preview in stable term and target order. */
  public String listBindingCandidates() {
    return """
        <script>
        SELECT term_id AS termId, target_type AS targetType, target_id AS targetId FROM %s
        WHERE metalake_id=#{metalakeId} AND deleted_at=0 AND term_id IN
        <foreach collection="termIds" item="id" open="(" separator="," close=")">#{id}</foreach>
        <if test="after != null">
          AND (target_type &gt; #{after.targetType}
            OR (target_type=#{after.targetType} AND target_id &gt; #{after.targetId}))
        </if>
        ORDER BY term_id, target_type, target_id LIMIT #{limit}
        </script>
        """
        .formatted(TABLE);
  }
}
