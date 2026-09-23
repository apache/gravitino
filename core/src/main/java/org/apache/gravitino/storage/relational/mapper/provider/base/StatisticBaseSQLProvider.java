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

import static org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper.STATISTIC_META_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.ibatis.annotations.Param;

public class StatisticBaseSQLProvider {

  /** Inserts a new live statistic without upsert fallback. */
  public String insertStatisticPO(@Param("statisticPO") StatisticPO statisticPO) {
    return "INSERT INTO "
        + STATISTIC_META_TABLE_NAME
        + " (statistic_id, statistic_name, statistic_value, metalake_id, metadata_object_id,"
        + " metadata_object_type, audit_info, current_version, last_version, deleted_at) VALUES"
        + " (#{statisticPO.statisticId}, #{statisticPO.statisticName},"
        + " #{statisticPO.statisticValue}, #{statisticPO.metalakeId},"
        + " #{statisticPO.metadataObjectId}, #{statisticPO.metadataObjectType},"
        + " #{statisticPO.auditInfo}, 1, 1, 0)";
  }

  /** Updates the value and advances the version when the observed row still matches. */
  public String updateStatisticPOWithVersion(
      @Param("statisticPO") StatisticPO statisticPO, @Param("previous") StatisticPO previous) {
    return "UPDATE "
        + STATISTIC_META_TABLE_NAME
        + " SET statistic_value = #{statisticPO.statisticValue},"
        + " audit_info = #{statisticPO.auditInfo},"
        + " last_version = current_version, current_version = current_version + 1"
        + " WHERE statistic_id = #{previous.statisticId}"
        + " AND metadata_object_id = #{previous.metadataObjectId}"
        + " AND statistic_name = #{previous.statisticName}"
        + " AND current_version = #{previous.currentVersion} AND deleted_at = 0";
  }

  /** Soft-deletes the observed statistic and advances its version. */
  public String deleteStatisticPOWithVersion(@Param("previous") StatisticPO previous) {
    return "UPDATE "
        + STATISTIC_META_TABLE_NAME
        + softDeleteSQL()
        + ", last_version = current_version, current_version = current_version + 1"
        + " WHERE statistic_id = #{previous.statisticId}"
        + " AND metadata_object_id = #{previous.metadataObjectId}"
        + " AND current_version = #{previous.currentVersion} AND deleted_at = 0";
  }

  public String softDeleteStatisticsByEntityId(@Param("entityId") Long entityId) {
    return "UPDATE "
        + STATISTIC_META_TABLE_NAME
        + softDeleteSQL()
        + " WHERE metadata_object_id = #{entityId} AND deleted_at = 0";
  }

  public String listStatisticPOsByEntityId(
      @Param("metalakeId") Long metalakeId, @Param("entityId") Long entityId) {
    return "SELECT statistic_id as statisticId, statistic_name as statisticName, metalake_id as metalakeId,"
        + " statistic_value as statisticValue, metadata_object_id as metadataObjectId,"
        + " metadata_object_type as metadataObjectType, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion, deleted_at as deletedAt FROM "
        + STATISTIC_META_TABLE_NAME
        + " WHERE metadata_object_id = #{entityId} AND deleted_at = 0 AND metalake_id = #{metalakeId}";
  }

  /** Selects the requested live statistics without loading unrelated values or audit fields. */
  public String listStatisticPOsByNames(
      @Param("metalakeId") Long metalakeId,
      @Param("entityId") Long entityId,
      @Param("names") List<String> names) {
    return "<script>"
        + listStatisticPOsByEntityId(metalakeId, entityId)
        + " AND statistic_name IN "
        + "<foreach collection='names' item='name' open='(' separator=',' close=')'>"
        + "#{name}"
        + "</foreach>"
        + "</script>";
  }

  public String softDeleteStatisticsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + STATISTIC_META_TABLE_NAME
        + " stat "
        + softDeleteSQL()
        + " WHERE stat.metalake_id = #{metalakeId} AND stat.deleted_at = 0";
  }

  public String softDeleteStatisticsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + STATISTIC_META_TABLE_NAME
        + " stat "
        + softDeleteSQL()
        + " WHERE stat.deleted_at = 0 AND EXISTS ("
        + " SELECT ct.catalog_id FROM "
        + CatalogMetaMapper.TABLE_NAME
        + " ct WHERE ct.catalog_id = #{catalogId} AND"
        + " ct.catalog_id = stat.metadata_object_id AND stat.metadata_object_type = 'CATALOG'"
        + " UNION"
        + " SELECT st.catalog_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.catalog_id = #{catalogId} AND"
        + " st.schema_id = stat.metadata_object_id AND stat.metadata_object_type = 'SCHEMA'"
        + " UNION"
        + " SELECT tt.catalog_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.catalog_id = #{catalogId} AND"
        + " tt.topic_id = stat.metadata_object_id AND stat.metadata_object_type = 'TOPIC'"
        + " UNION"
        + " SELECT tat.catalog_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.catalog_id = #{catalogId} AND"
        + " tat.table_id = stat.metadata_object_id AND stat.metadata_object_type = 'TABLE'"
        + " UNION"
        + " SELECT ft.catalog_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.catalog_id = #{catalogId} AND"
        + " ft.fileset_id = stat.metadata_object_id AND stat.metadata_object_type = 'FILESET'"
        + " UNION"
        + " SELECT mt.catalog_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.catalog_id = #{catalogId} AND"
        + " mt.model_id = stat.metadata_object_id AND stat.metadata_object_type = 'MODEL'"
        + ")";
  }

  public String softDeleteStatisticsBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>"
        + "UPDATE "
        + STATISTIC_META_TABLE_NAME
        + " stat "
        + softDeleteSQL()
        + " WHERE stat.deleted_at = 0 AND EXISTS ("
        + " SELECT st.schema_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND st.schema_id = stat.metadata_object_id AND stat.metadata_object_type = 'SCHEMA'"
        + " UNION"
        + " SELECT tt.schema_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND tt.topic_id = stat.metadata_object_id AND stat.metadata_object_type = 'TOPIC'"
        + " UNION"
        + " SELECT tat.schema_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND tat.table_id = stat.metadata_object_id AND stat.metadata_object_type = 'TABLE'"
        + " UNION"
        + " SELECT ft.schema_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND ft.fileset_id = stat.metadata_object_id AND stat.metadata_object_type = 'FILESET'"
        + " UNION"
        + " SELECT mt.schema_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.schema_id IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + " AND mt.model_id = stat.metadata_object_id AND stat.metadata_object_type = 'MODEL'"
        + ")"
        + "</script>";
  }

  public String deleteStatisticsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + STATISTIC_META_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  protected String softDeleteSQL() {
    return " SET deleted_at = " + DatabaseTimeSQL.MYSQL + " ";
  }
}
