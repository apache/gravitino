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
import org.apache.gravitino.storage.relational.po.PolicyMetadataObjectRelPO;
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface PolicyMetadataObjectRelMapper {
  String POLICY_METADATA_OBJECT_RELATION_TABLE_NAME = "policy_relation_meta";

  @Results({
    @Result(property = "policyId", column = "policy_id"),
    @Result(property = "policyName", column = "policy_name"),
    @Result(property = "policyType", column = "policy_type"),
    @Result(property = "metalakeId", column = "metalake_id"),
    @Result(property = "auditInfo", column = "audit_info"),
    @Result(property = "currentVersion", column = "current_version"),
    @Result(property = "lastVersion", column = "last_version"),
    @Result(property = "deletedAt", column = "deleted_at"),
    @Result(property = "policyVersionPO.id", column = "id"),
    @Result(property = "policyVersionPO.metalakeId", column = "version_metalake_id"),
    @Result(property = "policyVersionPO.policyId", column = "version_policy_id"),
    @Result(property = "policyVersionPO.version", column = "version"),
    @Result(property = "policyVersionPO.policyComment", column = "policy_comment"),
    @Result(property = "policyVersionPO.enabled", column = "enabled"),
    @Result(property = "policyVersionPO.content", column = "content"),
    @Result(property = "policyVersionPO.deletedAt", column = "version_deleted_at")
  })
  @SelectProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "listPolicyPOsByMetadataObjectIdAndType")
  List<PolicyPO> listPolicyPOsByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType);

  @Results({
    @Result(property = "policyId", column = "policy_id"),
    @Result(property = "policyName", column = "policy_name"),
    @Result(property = "policyType", column = "policy_type"),
    @Result(property = "metalakeId", column = "metalake_id"),
    @Result(property = "auditInfo", column = "audit_info"),
    @Result(property = "currentVersion", column = "current_version"),
    @Result(property = "lastVersion", column = "last_version"),
    @Result(property = "deletedAt", column = "deleted_at"),
    @Result(property = "policyVersionPO.id", column = "id"),
    @Result(property = "policyVersionPO.metalakeId", column = "version_metalake_id"),
    @Result(property = "policyVersionPO.policyId", column = "version_policy_id"),
    @Result(property = "policyVersionPO.version", column = "version"),
    @Result(property = "policyVersionPO.policyComment", column = "policy_comment"),
    @Result(property = "policyVersionPO.enabled", column = "enabled"),
    @Result(property = "policyVersionPO.content", column = "content"),
    @Result(property = "policyVersionPO.deletedAt", column = "version_deleted_at")
  })
  @SelectProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "getPolicyPOsByMetadataObjectAndPolicyName")
  PolicyPO getPolicyPOsByMetadataObjectAndPolicyName(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("policyName") String policyName);

  @SelectProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "listPolicyMetadataObjectRelsByMetalakeAndPolicyName")
  List<PolicyMetadataObjectRelPO> listPolicyMetadataObjectRelsByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName);

  @InsertProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "batchInsertPolicyMetadataObjectRels")
  void batchInsertPolicyMetadataObjectRels(
      @Param("policyRels") List<PolicyMetadataObjectRelPO> policyRelPOs);

  @UpdateProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "batchDeletePolicyMetadataObjectRelsByPolicyIdsAndMetadataObject")
  void batchDeletePolicyMetadataObjectRelsByPolicyIdsAndMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("policyIds") List<Long> policyIds);

  @UpdateProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "softDeletePolicyMetadataObjectRelsByMetalakeAndPolicyName")
  Integer softDeletePolicyMetadataObjectRelsByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName);

  @UpdateProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "softDeletePolicyMetadataObjectRelsByMetalakeId")
  void softDeletePolicyMetadataObjectRelsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "softDeletePolicyMetadataObjectRelsByMetadataObject")
  void softDeletePolicyMetadataObjectRelsByMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType);

  @UpdateProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "softDeletePolicyMetadataObjectRelsByCatalogId")
  void softDeletePolicyMetadataObjectRelsByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "softDeletePolicyMetadataObjectRelsBySchemaId")
  void softDeletePolicyMetadataObjectRelsBySchemaId(@Param("schemaId") Long schemaId);

  @UpdateProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "softDeletePolicyMetadataObjectRelsByTableId")
  void softDeletePolicyMetadataObjectRelsByTableId(@Param("tableId") Long tableId);

  @DeleteProvider(
      type = PolicyMetadataObjectRelSQLProviderFactory.class,
      method = "deletePolicyEntityRelsByLegacyTimeline")
  Integer deletePolicyEntityRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
