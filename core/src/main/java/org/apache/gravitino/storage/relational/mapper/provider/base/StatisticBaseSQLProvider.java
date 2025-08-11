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
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.ibatis.annotations.Param;

public class StatisticBaseSQLProvider {

  public String batchInsertStatisticPOsOnDuplicateKeyUpdate(
      @Param("statisticPOs") List<StatisticPO> statisticPOs) {
    return "<script>"
        + "INSERT INTO "
        + STATISTIC_META_TABLE_NAME
        + " (statistic_id, statistic_name, statistic_value, metalake_id, metadata_object_id,"
        + " metadata_object_type, audit_info, current_version, last_version, deleted_at) VALUES "
        + "<foreach collection='statisticPOs' item='item' separator=','>"
        + "(#{item.statisticId}, "
        + "#{item.statisticName}, "
        + "#{item.statisticValue}, "
        + "#{item.metalakeId}, "
        + "#{item.metadataObjectId}, "
        + "#{item.metadataObjectType}, "
        + "#{item.auditInfo}, "
        + "#{item.currentVersion}, "
        + "#{item.lastVersion}, "
        + "#{item.deletedAt})"
        + "</foreach>"
        + " ON DUPLICATE KEY UPDATE "
        + "  statistic_value = VALUES(statistic_value),"
        + "  audit_info = VALUES(audit_info),"
        + "  current_version = VALUES(current_version),"
        + "  last_version = VALUES(last_version),"
        + "  deleted_at = VALUES(deleted_at)"
        + "</script>";
  }

  public String batchDeleteStatisticPOs(
      @Param("entityId") Long entityId, @Param("statisticNames") List<String> statisticNames) {
    return "<script>"
        + "UPDATE "
        + STATISTIC_META_TABLE_NAME
        + softDeleteSQL()
        + " WHERE "
        + " statistic_name IN  ("
        + "<foreach collection='statisticNames' item='item' separator=','>"
        + " #{item}"
        + "</foreach>"
        + " ) AND deleted_at = 0 AND metadata_object_id = #{entityId}"
        + "</script>";
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
        + "metadata_object_type as metadataObjectType, audit_info as auditInfo,"
        + "current_version as currentVersion, last_version as lastVersion, deleted_at as deletedAt FROM "
        + STATISTIC_META_TABLE_NAME
        + " WHERE metadata_object_id = #{entityId} AND deleted_at = 0 AND metalake_id = #{metalakeId}";
  }

  public String softDeleteStatisticsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + STATISTIC_META_TABLE_NAME
        + " stat "
        + softDeleteSQL()
        + " WHERE  stat.metalake_id = #{metalakeId} AND stat.deleted_at = 0";
  }

  public String softDeleteStatisticsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + STATISTIC_META_TABLE_NAME
        + " stat "
        + softDeleteSQL()
        + " WHERE stat.deleted_at = 0 AND EXISTS ("
        + " SELECT ct.catalog_id FROM "
        + CatalogMetaMapper.TABLE_NAME
        + " ct WHERE ct.catalog_id = #{catalogId}  AND "
        + " ct.catalog_id = stat.metadata_object_id AND stat.metadata_object_type = 'CATALOG'"
        + " UNION "
        + " SELECT st.catalog_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.catalog_id = #{catalogId} AND "
        + " st.schema_id = stat.metadata_object_id AND stat.metadata_object_type = 'SCHEMA'"
        + " UNION "
        + " SELECT tt.catalog_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.catalog_id = #{catalogId} AND "
        + " tt.topic_id = stat.metadata_object_id AND stat.metadata_object_type = 'TOPIC'"
        + " UNION "
        + " SELECT tat.catalog_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.catalog_id = #{catalogId} AND "
        + " tat.table_id = stat.metadata_object_id AND stat.metadata_object_type = 'TABLE'"
        + " UNION "
        + " SELECT ft.catalog_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.catalog_id = #{catalogId} AND"
        + " ft.fileset_id = stat.metadata_object_id AND stat.metadata_object_type = 'FILESET'"
        + " UNION "
        + " SELECT mt.catalog_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.catalog_id = #{catalogId} AND"
        + " mt.model_id = stat.metadata_object_id AND stat.metadata_object_type = 'MODEL'"
        + ")";
  }

  public String softDeleteStatisticsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + STATISTIC_META_TABLE_NAME
        + " stat"
        + softDeleteSQL()
        + " WHERE stat.deleted_at = 0 AND EXISTS ("
        + " SELECT st.schema_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.schema_id = #{schemaId} "
        + " AND st.schema_id = stat.metadata_object_id AND stat.metadata_object_type = 'SCHEMA'"
        + " UNION "
        + " SELECT tt.schema_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.schema_id = #{schemaId} AND "
        + " tt.topic_id = stat.metadata_object_id AND stat.metadata_object_type = 'TOPIC'"
        + " UNION "
        + " SELECT tat.schema_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.schema_id = #{schemaId} AND "
        + " tat.table_id = stat.metadata_object_id AND stat.metadata_object_type = 'TABLE'"
        + " UNION "
        + " SELECT ft.schema_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.schema_id = #{schemaId} AND "
        + " ft.fileset_id = stat.metadata_object_id AND stat.metadata_object_type = 'FILESET'"
        + " UNION "
        + " SELECT mt.schema_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.schema_id = #{schemaId} AND "
        + " mt.model_id = stat.metadata_object_id AND stat.metadata_object_type = 'MODEL'"
        + ")";
  }

  public String deleteStatisticsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + STATISTIC_META_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  protected String softDeleteSQL() {
    return " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000 ";
  }
}
