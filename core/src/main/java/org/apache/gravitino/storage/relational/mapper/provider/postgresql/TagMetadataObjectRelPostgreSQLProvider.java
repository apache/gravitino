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

import static org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.TagMetadataObjectRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class TagMetadataObjectRelPostgreSQLProvider extends TagMetadataObjectRelBaseSQLProvider {
  @Override
  public String softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
      String metalakeName, String tagName) {
    return "UPDATE "
        + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " te SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE te.tag_id IN (SELECT tm.tag_id FROM "
        + TagMetaMapper.TAG_TABLE_NAME
        + " tm WHERE tm.metalake_id IN (SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND tm.tag_name = #{tagName} AND tm.deleted_at = 0) AND te.deleted_at = 0";
  }

  @Override
  public String softDeleteTagMetadataObjectRelsByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " te SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE EXISTS (SELECT * FROM "
        + TagMetaMapper.TAG_TABLE_NAME
        + " tm WHERE tm.metalake_id = #{metalakeId} AND tm.tag_id = te.tag_id"
        + " AND tm.deleted_at = 0) AND te.deleted_at = 0";
  }

  @Override
  public String softDeleteTagMetadataObjectRelsByMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return " UPDATE "
        + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE metadata_object_id = #{metadataObjectId} AND deleted_at = 0"
        + " AND metadata_object_type = #{metadataObjectType}";
  }

  @Override
  public String softDeleteTagMetadataObjectRelsByCatalogId(@Param("catalogId") Long catalogId) {
    return " UPDATE "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " tmt SET deleted_at =  floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE tmt.deleted_at = 0 AND EXISTS ("
        + " SELECT ct.catalog_id FROM "
        + CatalogMetaMapper.TABLE_NAME
        + " ct WHERE ct.catalog_id = #{catalogId}  AND "
        + "ct.catalog_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'CATALOG'"
        + " UNION "
        + " SELECT st.catalog_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.catalog_id = #{catalogId} AND "
        + "st.schema_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'SCHEMA'"
        + " UNION "
        + " SELECT tt.catalog_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.catalog_id = #{catalogId} AND "
        + "tt.topic_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'TOPIC'"
        + " UNION "
        + " SELECT tat.catalog_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.catalog_id = #{catalogId} AND "
        + "tat.table_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'TABLE'"
        + " UNION "
        + " SELECT ft.catalog_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.catalog_id = #{catalogId}  AND"
        + " ft.fileset_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'FILESET'"
        + " UNION "
        + " SELECT cot.catalog_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " cot WHERE cot.catalog_id = #{catalogId} AND"
        + " cot.column_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'COLUMN'"
        + ")";
  }

  @Override
  public String softDeleteTagMetadataObjectRelsBySchemaId(@Param("schemaId") Long schemaId) {
    return " UPDATE "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " tmt SET deleted_at =  floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE tmt.deleted_at = 0 AND EXISTS ("
        + " SELECT st.schema_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.schema_id = #{schemaId} AND "
        + "st.schema_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'SCHEMA'"
        + " UNION "
        + " SELECT tt.schema_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.schema_id = #{schemaId} AND "
        + "tt.topic_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'TOPIC'"
        + " UNION "
        + " SELECT tat.schema_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.schema_id = #{schemaId} AND"
        + " tat.table_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'TABLE'"
        + " UNION "
        + " SELECT ft.schema_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.schema_id = #{schemaId} AND"
        + " ft.fileset_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'FILESET'"
        + " UNION "
        + " SELECT cot.schema_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " cot WHERE cot.schema_id = #{schemaId} AND"
        + " cot.column_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'COLUMN'"
        + ")";
  }

  @Override
  public String softDeleteTagMetadataObjectRelsByTableId(@Param("tableId") Long tableId) {
    return " UPDATE "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " tmt SET deleted_at =  floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE tmt.deleted_at = 0 AND EXISTS ("
        + " SELECT tat.table_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.table_id = #{tableId} AND "
        + " tat.table_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'TABLE'"
        + " UNION "
        + " SELECT cot.table_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " cot WHERE cot.table_id = #{tableId} AND "
        + " cot.column_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'COLUMN'"
        + ")";
  }

  @Override
  public String batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
      Long metadataObjectId, String metadataObjectType, List<Long> tagIds) {
    return "<script>"
        + "UPDATE "
        + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE tag_id IN "
        + "<foreach item='tagId' collection='tagIds' open='(' separator=',' close=')'>"
        + "#{tagId}"
        + "</foreach>"
        + " And metadata_object_id = #{metadataObjectId}"
        + " AND metadata_object_type = #{metadataObjectType} AND deleted_at = 0"
        + "</script>";
  }

  @Override
  public String listTagMetadataObjectRelsByMetalakeAndTagName(String metalakeName, String tagName) {
    return "SELECT te.tag_id as tagId, te.metadata_object_id as metadataObjectId,"
        + " te.metadata_object_type as metadataObjectType, te.audit_info as auditInfo,"
        + " te.current_version as currentVersion, te.last_version as lastVersion,"
        + " te.deleted_at as deletedAt"
        + " FROM "
        + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " te JOIN "
        + TagMetaMapper.TAG_TABLE_NAME
        + " tm ON te.tag_id = tm.tag_id JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON tm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName} AND tm.tag_name = #{tagName}"
        + " AND te.deleted_at = 0 AND tm.deleted_at = 0 AND mm.deleted_at = 0";
  }

  @Override
  public String deleteTagEntityRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
