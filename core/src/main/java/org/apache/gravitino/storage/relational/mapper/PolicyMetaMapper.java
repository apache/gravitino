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
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface PolicyMetaMapper {
  String POLICY_META_TABLE_NAME = "policy_meta";

  @Results({
    @Result(property = "policyId", column = "policy_id"),
    @Result(property = "policyName", column = "policy_name"),
    @Result(property = "policyType", column = "policy_type"),
    @Result(property = "metalakeId", column = "metalake_id"),
    @Result(property = "inheritable", column = "inheritable"),
    @Result(property = "exclusive", column = "exclusive"),
    @Result(property = "supportedObjectTypes", column = "supported_object_types"),
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
  @SelectProvider(type = PolicyMetaSQLProviderFactory.class, method = "listPolicyPOsByMetalake")
  List<PolicyPO> listPolicyPOsByMetalake(@Param("metalakeName") String metalakeName);

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
      type = PolicyMetaSQLProviderFactory.class,
      method = "listPolicyPOsByMetalakeAndPolicyNames")
  List<PolicyPO> listPolicyPOsByMetalakeAndPolicyNames(
      @Param("metalakeName") String metalakeName, @Param("policyNames") List<String> policyNames);

  @InsertProvider(
      type = PolicyMetaSQLProviderFactory.class,
      method = "insertPolicyMetaOnDuplicateKeyUpdate")
  void insertPolicyMetaOnDuplicateKeyUpdate(@Param("policyMeta") PolicyPO policyPO);

  @InsertProvider(type = PolicyMetaSQLProviderFactory.class, method = "insertPolicyMeta")
  void insertPolicyMeta(@Param("policyMeta") PolicyPO policyPO);

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
      type = PolicyMetaSQLProviderFactory.class,
      method = "selectPolicyMetaByMetalakeAndName")
  PolicyPO selectPolicyMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName);

  @UpdateProvider(type = PolicyMetaSQLProviderFactory.class, method = "updatePolicyMeta")
  Integer updatePolicyMeta(
      @Param("newPolicyMeta") PolicyPO newPolicyMeta,
      @Param("oldPolicyMeta") PolicyPO oldPolicyMeta);

  @UpdateProvider(
      type = PolicyMetaSQLProviderFactory.class,
      method = "softDeletePolicyByMetalakeAndPolicyName")
  Integer softDeletePolicyByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName);

  @UpdateProvider(
      type = PolicyMetaSQLProviderFactory.class,
      method = "softDeletePolicyMetasByMetalakeId")
  void softDeletePolicyMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = PolicyMetaSQLProviderFactory.class,
      method = "deletePolicyMetasByLegacyTimeline")
  Integer deletePolicyMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
