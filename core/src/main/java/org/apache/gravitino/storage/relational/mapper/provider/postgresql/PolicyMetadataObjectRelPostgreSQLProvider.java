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

import static org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.PolicyMetadataObjectRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class PolicyMetadataObjectRelPostgreSQLProvider
    extends PolicyMetadataObjectRelBaseSQLProvider {
  private static final String DELETED_AT_NOW_EXPRESSION =
      " floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) ";

  @Override
  public String softDeletePolicyMetadataObjectRelsByMetalakeAndPolicyName(
      String metalakeName, String policyName) {
    return "UPDATE "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " te SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " WHERE te.policy_id IN (SELECT tm.policy_id FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " tm WHERE tm.metalake_id IN (SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND tm.policy_name = #{policyName} AND tm.deleted_at = 0) AND te.deleted_at = 0";
  }

  @Override
  public String softDeletePolicyMetadataObjectRelsByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " te SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " WHERE te.policy_id IN (SELECT tm.policy_id FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " tm WHERE tm.metalake_id = #{metalakeId} AND tm.deleted_at = 0) AND te.deleted_at = 0";
  }

  @Override
  public String softDeletePolicyMetadataObjectRelsByMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return "UPDATE "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " WHERE metadata_object_id = #{metadataObjectId} AND deleted_at = 0"
        + " AND metadata_object_type = #{metadataObjectType}";
  }

  @Override
  public String softDeletePolicyMetadataObjectRelsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " pe SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " FROM "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " pe_alias"
        + " LEFT JOIN "
        + CatalogMetaMapper.TABLE_NAME
        + " ct ON pe_alias.metadata_object_id = ct.catalog_id AND pe_alias.metadata_object_type = 'CATALOG'"
        + " LEFT JOIN "
        + SchemaMetaMapper.TABLE_NAME
        + " st ON pe_alias.metadata_object_id = st.schema_id AND pe_alias.metadata_object_type = 'SCHEMA'"
        + " LEFT JOIN "
        + TopicMetaMapper.TABLE_NAME
        + " tt ON pe_alias.metadata_object_id = tt.topic_id AND pe_alias.metadata_object_type = 'TOPIC'"
        + " LEFT JOIN "
        + TableMetaMapper.TABLE_NAME
        + " tat ON pe_alias.metadata_object_id = tat.table_id AND pe_alias.metadata_object_type = 'TABLE'"
        + " LEFT JOIN "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft ON pe_alias.metadata_object_id = ft.fileset_id AND pe_alias.metadata_object_type = 'FILESET'"
        + " LEFT JOIN "
        + ModelMetaMapper.TABLE_NAME
        + " mt ON pe_alias.metadata_object_id = mt.model_id AND pe_alias.metadata_object_type = 'MODEL'"
        + " WHERE pe.id = pe_alias.id AND pe.deleted_at = 0 AND ("
        + "   ct.catalog_id = #{catalogId} OR st.catalog_id = #{catalogId} OR tt.catalog_id = #{catalogId}"
        + "   OR tat.catalog_id = #{catalogId} OR ft.catalog_id = #{catalogId} OR mt.catalog_id = #{catalogId}"
        + " )";
  }

  @Override
  public String softDeletePolicyMetadataObjectRelsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " pe SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " FROM "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " pe_alias"
        + " LEFT JOIN "
        + SchemaMetaMapper.TABLE_NAME
        + " st ON pe_alias.metadata_object_id = st.schema_id AND pe_alias.metadata_object_type = 'SCHEMA'"
        + " LEFT JOIN "
        + TopicMetaMapper.TABLE_NAME
        + " tt ON pe_alias.metadata_object_id = tt.topic_id AND pe_alias.metadata_object_type = 'TOPIC'"
        + " LEFT JOIN "
        + TableMetaMapper.TABLE_NAME
        + " tat ON pe_alias.metadata_object_id = tat.table_id AND pe_alias.metadata_object_type = 'TABLE'"
        + " LEFT JOIN "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft ON pe_alias.metadata_object_id = ft.fileset_id AND pe_alias.metadata_object_type = 'FILESET'"
        + " LEFT JOIN "
        + ModelMetaMapper.TABLE_NAME
        + " mt ON pe_alias.metadata_object_id = mt.model_id AND pe_alias.metadata_object_type = 'MODEL'"
        + " WHERE pe.id = pe_alias.id AND pe.deleted_at = 0 AND ("
        + "   st.schema_id = #{schemaId} OR tt.schema_id = #{schemaId} OR tat.schema_id = #{schemaId}"
        + "   OR ft.schema_id = #{schemaId} OR mt.schema_id = #{schemaId}"
        + " )";
  }

  @Override
  public String softDeletePolicyMetadataObjectRelsByTableId(@Param("tableId") Long tableId) {
    return "UPDATE "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " WHERE deleted_at = 0 AND ("
        + "   (metadata_object_id = #{tableId} AND metadata_object_type = 'TABLE') OR "
        + "   metadata_object_id IN (SELECT column_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " WHERE table_id = #{tableId} AND deleted_at = 0)"
        + " AND metadata_object_type = 'COLUMN'"
        + ")";
  }

  @Override
  public String batchDeletePolicyMetadataObjectRelsByPolicyIdsAndMetadataObject(
      Long metadataObjectId, String metadataObjectType, List<Long> policyIds) {
    return "<script>"
        + "UPDATE "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " WHERE policy_id IN "
        + "<foreach item='policyId' collection='policyIds' open='(' separator=',' close=')'>"
        + "#{policyId}"
        + "</foreach>"
        + " AND metadata_object_id = #{metadataObjectId}"
        + " AND metadata_object_type = #{metadataObjectType} AND deleted_at = 0"
        + "</script>";
  }

  @Override
  public String listPolicyMetadataObjectRelsByMetalakeAndPolicyName(
      String metalakeName, String policyName) {
    return "SELECT te.policy_id as policyId, te.metadata_object_id as metadataObjectId,"
        + " te.metadata_object_type as metadataObjectType, te.audit_info as auditInfo,"
        + " te.current_version as currentVersion, te.last_version as lastVersion,"
        + " te.deleted_at as deletedAt"
        + " FROM "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " te JOIN "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " tm ON te.policy_id = tm.policy_id JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON tm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName} AND tm.policy_name = #{policyName}"
        + " AND te.deleted_at = 0 AND tm.deleted_at = 0 AND mm.deleted_at = 0";
  }

  @Override
  public String deletePolicyEntityRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
