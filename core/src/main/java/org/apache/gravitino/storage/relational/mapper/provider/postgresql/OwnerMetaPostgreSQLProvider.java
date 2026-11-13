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

import static org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper.OWNER_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.mapper.provider.MetadataObjectSQL;
import org.apache.gravitino.storage.relational.mapper.provider.base.OwnerMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.OwnerRelForDeletion;
import org.apache.ibatis.annotations.Param;

public class OwnerMetaPostgreSQLProvider extends OwnerMetaBaseSQLProvider {
  @Override
  protected String currentTimestampMillisExpression() {
    return DatabaseTimeSQL.POSTGRESQL;
  }

  @Override
  public String softDeleteOwnerRelByMetadataObjectIdAndType(
      Long metadataObjectId, String metadataObjectType) {
    String now = currentTimestampMillisExpression();
    return "UPDATE "
        + OWNER_TABLE_NAME
        + " SET deleted_at = "
        + newerRetiredAtExpression(now)
        + ", updated_at = "
        + now
        + " WHERE metadata_object_id = #{metadataObjectId} AND metadata_object_type = #{metadataObjectType} AND deleted_at = 0";
  }

  /**
   * Returns a deleted_at that is newer than every retired owner row of the same object. The
   * PostgreSQL clock is the transaction start time, so owner changes that start in one millisecond
   * would otherwise repeat a retired row and break the unique key. Owner writes on one object are
   * serialized by its row lock, so reading the maximum here cannot race another owner change.
   */
  private String newerRetiredAtExpression(String now) {
    return "GREATEST("
        + now
        + ", (SELECT COALESCE(MAX(retired.deleted_at), 0) + 1 FROM "
        + OWNER_TABLE_NAME
        + " retired WHERE retired.metadata_object_id = "
        + OWNER_TABLE_NAME
        + ".metadata_object_id AND retired.metadata_object_type = "
        + OWNER_TABLE_NAME
        + ".metadata_object_type))";
  }

  @Override
  public String softDeleteOwnerRelByOwnerIdAndType(Long ownerId, String ownerType) {
    String now = currentTimestampMillisExpression();
    return "UPDATE "
        + OWNER_TABLE_NAME
        + " SET deleted_at = "
        + now
        + ", updated_at = "
        + now
        + " WHERE owner_id = #{ownerId} AND owner_type = #{ownerType} AND deleted_at = 0";
  }

  @Override
  public String softDeleteOwnerRelByMetalakeId(Long metalakeId) {
    String now = currentTimestampMillisExpression();
    return "UPDATE "
        + OWNER_TABLE_NAME
        + " SET deleted_at = "
        + now
        + ", updated_at = "
        + now
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteOwnerRelByCatalogId(Long catalogId) {
    String now = currentTimestampMillisExpression();
    return "UPDATE "
        + OWNER_TABLE_NAME
        + " ot SET deleted_at = "
        + now
        + ", updated_at = "
        + now
        + " WHERE ot.deleted_at = 0 AND EXISTS ("
        + " SELECT ct.catalog_id FROM "
        + CatalogMetaMapper.TABLE_NAME
        + " ct WHERE ct.catalog_id = #{catalogId} AND"
        + " ct.catalog_id = ot.metadata_object_id AND ot.metadata_object_type = 'CATALOG'"
        + " UNION"
        + " SELECT st.catalog_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.catalog_id = #{catalogId} AND"
        + " st.schema_id = ot.metadata_object_id AND ot.metadata_object_type = 'SCHEMA'"
        + MetadataObjectSQL.excludeOtherActiveCatalogs(
            SchemaMetaMapper.TABLE_NAME, "schema_id", "st")
        + " UNION"
        + " SELECT tt.catalog_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.catalog_id = #{catalogId} AND"
        + " tt.topic_id = ot.metadata_object_id AND ot.metadata_object_type = 'TOPIC'"
        + MetadataObjectSQL.excludeOtherActiveCatalogs(TopicMetaMapper.TABLE_NAME, "topic_id", "tt")
        + " UNION"
        + " SELECT tat.catalog_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.catalog_id = #{catalogId} AND"
        + " tat.table_id = ot.metadata_object_id AND ot.metadata_object_type = 'TABLE'"
        + MetadataObjectSQL.excludeOtherActiveCatalogs(
            TableMetaMapper.TABLE_NAME, "table_id", "tat")
        + " UNION"
        + " SELECT ft.catalog_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.catalog_id = #{catalogId} AND"
        + " ft.fileset_id = ot.metadata_object_id AND ot.metadata_object_type = 'FILESET'"
        + MetadataObjectSQL.excludeOtherActiveCatalogs(
            FilesetMetaMapper.META_TABLE_NAME, "fileset_id", "ft")
        + " UNION"
        + " SELECT mt.catalog_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.catalog_id = #{catalogId} AND"
        + " mt.model_id = ot.metadata_object_id AND ot.metadata_object_type = 'MODEL'"
        + MetadataObjectSQL.excludeOtherActiveCatalogs(ModelMetaMapper.TABLE_NAME, "model_id", "mt")
        + " UNION"
        + " SELECT vt.catalog_id FROM "
        + ViewMetaMapper.TABLE_NAME
        + " vt WHERE vt.catalog_id = #{catalogId} AND"
        + " vt.view_id = ot.metadata_object_id AND ot.metadata_object_type = 'VIEW'"
        + MetadataObjectSQL.excludeOtherActiveCatalogs(ViewMetaMapper.TABLE_NAME, "view_id", "vt")
        + " UNION"
        + " SELECT fnt.catalog_id FROM "
        + FunctionMetaMapper.TABLE_NAME
        + " fnt WHERE fnt.catalog_id = #{catalogId} AND"
        + " fnt.function_id = ot.metadata_object_id AND ot.metadata_object_type = 'FUNCTION'"
        + MetadataObjectSQL.excludeOtherActiveCatalogs(
            FunctionMetaMapper.TABLE_NAME, "function_id", "fnt")
        + " UNION"
        + " SELECT cot.catalog_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " cot WHERE cot.catalog_id = #{catalogId} AND"
        + " cot.column_id = ot.metadata_object_id AND ot.metadata_object_type = 'COLUMN'"
        + MetadataObjectSQL.excludeOtherActiveCatalogs(
            TableColumnMapper.COLUMN_TABLE_NAME, "column_id", "cot")
        + " UNION"
        + " SELECT smt.catalog_id FROM "
        + SemanticModelMetaMapper.TABLE_NAME
        + " smt WHERE smt.catalog_id = #{catalogId} AND"
        + " smt.semantic_model_id = ot.metadata_object_id AND ot.metadata_object_type = 'SEMANTIC_MODEL'"
        + MetadataObjectSQL.excludeOtherActiveCatalogs(
            SemanticModelMetaMapper.TABLE_NAME, "semantic_model_id", "smt")
        + ")";
  }

  @Override
  public String softDeleteOwnerRelBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    String now = currentTimestampMillisExpression();
    return "<script>"
        + "UPDATE "
        + OWNER_TABLE_NAME
        + " ot SET deleted_at = "
        + now
        + ", updated_at = "
        + now
        + " WHERE ot.deleted_at = 0 AND EXISTS ("
        + " SELECT st.schema_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND st.schema_id = ot.metadata_object_id AND ot.metadata_object_type = 'SCHEMA'"
        + " UNION"
        + " SELECT tt.schema_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND tt.topic_id = ot.metadata_object_id AND ot.metadata_object_type = 'TOPIC'"
        + MetadataObjectSQL.excludeOtherActiveSchemas(TopicMetaMapper.TABLE_NAME, "topic_id", "tt")
        + " UNION"
        + " SELECT tat.schema_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND tat.table_id = ot.metadata_object_id AND ot.metadata_object_type = 'TABLE'"
        + MetadataObjectSQL.excludeOtherActiveSchemas(TableMetaMapper.TABLE_NAME, "table_id", "tat")
        + " UNION"
        + " SELECT ft.schema_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND ft.fileset_id = ot.metadata_object_id AND ot.metadata_object_type = 'FILESET'"
        + MetadataObjectSQL.excludeOtherActiveSchemas(
            FilesetMetaMapper.META_TABLE_NAME, "fileset_id", "ft")
        + " UNION"
        + " SELECT mt.schema_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND mt.model_id = ot.metadata_object_id AND ot.metadata_object_type = 'MODEL'"
        + MetadataObjectSQL.excludeOtherActiveSchemas(ModelMetaMapper.TABLE_NAME, "model_id", "mt")
        + " UNION"
        + " SELECT vt.schema_id FROM "
        + ViewMetaMapper.TABLE_NAME
        + " vt WHERE vt.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND vt.view_id = ot.metadata_object_id AND ot.metadata_object_type = 'VIEW'"
        + MetadataObjectSQL.excludeOtherActiveSchemas(ViewMetaMapper.TABLE_NAME, "view_id", "vt")
        + " UNION"
        + " SELECT fnt.schema_id FROM "
        + FunctionMetaMapper.TABLE_NAME
        + " fnt WHERE fnt.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND fnt.function_id = ot.metadata_object_id AND ot.metadata_object_type = 'FUNCTION'"
        + MetadataObjectSQL.excludeOtherActiveSchemas(
            FunctionMetaMapper.TABLE_NAME, "function_id", "fnt")
        + " UNION"
        + " SELECT cot.schema_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " cot WHERE cot.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach> AND"
        + " cot.column_id = ot.metadata_object_id AND ot.metadata_object_type = 'COLUMN'"
        + MetadataObjectSQL.excludeOtherActiveSchemas(
            TableColumnMapper.COLUMN_TABLE_NAME, "column_id", "cot")
        + " UNION"
        + " SELECT smt.schema_id FROM "
        + SemanticModelMetaMapper.TABLE_NAME
        + " smt WHERE smt.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach> AND"
        + " smt.semantic_model_id = ot.metadata_object_id AND ot.metadata_object_type = 'SEMANTIC_MODEL'"
        + MetadataObjectSQL.excludeOtherActiveSchemas(
            SemanticModelMetaMapper.TABLE_NAME, "semantic_model_id", "smt")
        + ")"
        + "</script>";
  }

  @Override
  public String batchSoftDeleteOwnerRelByMetadataObjects(
      @Param("deletions") List<OwnerRelForDeletion> deletions) {
    String now = currentTimestampMillisExpression();
    return "<script>"
        + "UPDATE "
        + OWNER_TABLE_NAME
        + " SET deleted_at = "
        + newerRetiredAtExpression(now)
        + ", updated_at = "
        + now
        + " WHERE deleted_at = 0 AND ("
        + "<foreach collection='deletions' item='t' separator=' OR '>"
        + "(metadata_object_id = #{t.metadataObjectId} AND metadata_object_type = #{t.metadataObjectType})"
        + "</foreach>"
        + ")"
        + "</script>";
  }

  @Override
  public String deleteOwnerMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + OWNER_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + OWNER_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
