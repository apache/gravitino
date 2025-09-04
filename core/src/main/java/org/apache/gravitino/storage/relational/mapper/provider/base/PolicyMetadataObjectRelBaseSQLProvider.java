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

import static org.apache.gravitino.storage.relational.mapper.PolicyVersionMapper.POLICY_VERSION_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.po.PolicyMetadataObjectRelPO;
import org.apache.ibatis.annotations.Param;

public class PolicyMetadataObjectRelBaseSQLProvider {

  public String listPolicyPOsByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version,"
        + " pm.deleted_at, pvi.id, pvi.metalake_id as version_metalake_id, pvi.policy_id as version_policy_id,"
        + " pvi.version, pvi.policy_comment, pvi.enabled, pvi.content, pvi.deleted_at as version_deleted_at"
        + " FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm JOIN "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " pe ON pm.policy_id = pe.policy_id"
        + " JOIN "
        + POLICY_VERSION_TABLE_NAME
        + " pvi ON pm.policy_id = pvi.policy_id AND pm.current_version = pvi.version"
        + " WHERE pe.metadata_object_id = #{metadataObjectId}"
        + " AND pe.metadata_object_type = #{metadataObjectType} AND pe.deleted_at = 0"
        + " AND pm.deleted_at = 0 AND pvi.deleted_at = 0";
  }

  public String getPolicyPOsByMetadataObjectAndPolicyName(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("policyName") String policyName) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version,"
        + " pm.deleted_at, pvi.id, pvi.metalake_id as version_metalake_id, pvi.policy_id as version_policy_id,"
        + " pvi.version, pvi.policy_comment, pvi.enabled, pvi.content, pvi.deleted_at as version_deleted_at"
        + " FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm JOIN "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " pe ON pm.policy_id = pe.policy_id"
        + " JOIN "
        + POLICY_VERSION_TABLE_NAME
        + " pvi ON pm.policy_id = pvi.policy_id AND pm.current_version = pvi.version"
        + " WHERE pe.metadata_object_id = #{metadataObjectId}"
        + " AND pe.metadata_object_type = #{metadataObjectType} AND pm.policy_name = #{policyName}"
        + " AND pe.deleted_at = 0 AND pm.deleted_at = 0 AND pvi.deleted_at = 0";
  }

  public String listPolicyMetadataObjectRelsByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return "SELECT pe.policy_id as policyId, pe.metadata_object_id as metadataObjectId,"
        + " pe.metadata_object_type as metadataObjectType, pe.audit_info as auditInfo,"
        + " pe.current_version as currentVersion, pe.last_version as lastVersion,"
        + " pe.deleted_at as deletedAt"
        + " FROM "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " pe JOIN "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm ON pe.policy_id = pm.policy_id"
        + " JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON pm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName} AND pm.policy_name = #{policyName}"
        + " AND pe.deleted_at = 0 AND pm.deleted_at = 0 AND mm.deleted_at = 0";
  }

  public String batchInsertPolicyMetadataObjectRels(
      @Param("policyRels") List<PolicyMetadataObjectRelPO> policyRelPOs) {
    return "<script>"
        + "INSERT INTO "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + "(policy_id, metadata_object_id, metadata_object_type, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES "
        + "<foreach collection='policyRels' item='item' separator=','>"
        + "(#{item.policyId},"
        + " #{item.metadataObjectId},"
        + " #{item.metadataObjectType},"
        + " #{item.auditInfo},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})"
        + "</foreach>"
        + "</script>";
  }

  public String batchDeletePolicyMetadataObjectRelsByPolicyIdsAndMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("policyIds") List<Long> policyIds) {
    return "<script>"
        + "UPDATE "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE policy_id IN "
        + "<foreach item='policyId' collection='policyIds' open='(' separator=',' close=')'>"
        + "#{policyId}"
        + "</foreach>"
        + " AND metadata_object_id = #{metadataObjectId}"
        + " AND metadata_object_type = #{metadataObjectType} AND deleted_at = 0"
        + "</script>";
  }

  public String softDeletePolicyMetadataObjectRelsByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return "UPDATE "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " pe JOIN "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm ON pe.policy_id = pm.policy_id JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON pm.metalake_id = mm.metalake_id"
        + " SET pe.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE mm.metalake_name = #{metalakeName} AND pm.policy_name = #{policyName}"
        + " AND pe.deleted_at = 0 AND pm.deleted_at = 0 AND mm.deleted_at = 0";
  }

  public String softDeletePolicyMetadataObjectRelsByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " pe JOIN "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm ON pe.policy_id = pm.policy_id"
        + " SET pe.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE pm.metalake_id = #{metalakeId}"
        + " AND pe.deleted_at = 0 AND pm.deleted_at = 0";
  }

  public String softDeletePolicyMetadataObjectRelsByMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return " UPDATE "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metadata_object_id = #{metadataObjectId} AND deleted_at = 0"
        + " AND metadata_object_type = #{metadataObjectType}";
  }

  public String softDeletePolicyMetadataObjectRelsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0) + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE deleted_at = 0 AND ("
        + "   (metadata_object_type = 'CATALOG' AND metadata_object_id = #{catalogId})"
        + "   OR (metadata_object_type = 'SCHEMA' AND metadata_object_id IN (SELECT schema_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " WHERE catalog_id = #{catalogId}))"
        + "   OR (metadata_object_type = 'TOPIC' AND metadata_object_id IN (SELECT topic_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " WHERE catalog_id = #{catalogId}))"
        + "   OR (metadata_object_type = 'TABLE' AND metadata_object_id IN (SELECT table_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " WHERE catalog_id = #{catalogId}))"
        + "   OR (metadata_object_type = 'FILESET' AND metadata_object_id IN (SELECT fileset_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " WHERE catalog_id = #{catalogId}))"
        + "   OR (metadata_object_type = 'MODEL' AND metadata_object_id IN (SELECT model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE catalog_id = #{catalogId}))"
        + " )";
  }

  public String softDeletePolicyMetadataObjectRelsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0) + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE deleted_at = 0 AND ("
        + "   (metadata_object_type = 'SCHEMA' AND metadata_object_id = #{schemaId})"
        + "   OR (metadata_object_type = 'TOPIC' AND metadata_object_id IN (SELECT topic_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " WHERE schema_id = #{schemaId}))"
        + "   OR (metadata_object_type = 'TABLE' AND metadata_object_id IN (SELECT table_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " WHERE schema_id = #{schemaId}))"
        + "   OR (metadata_object_type = 'FILESET' AND metadata_object_id IN (SELECT fileset_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " WHERE schema_id = #{schemaId}))"
        + "   OR (metadata_object_type = 'MODEL' AND metadata_object_id IN (SELECT model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE schema_id = #{schemaId}))"
        + " )";
  }

  public String softDeletePolicyMetadataObjectRelsByTableId(@Param("tableId") Long tableId) {
    return "UPDATE "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metadata_object_id = #{tableId}"
        + " AND metadata_object_type = 'TABLE'"
        + " AND deleted_at = 0";
  }

  public String deletePolicyEntityRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
