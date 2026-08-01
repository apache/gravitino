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
import org.apache.gravitino.storage.relational.po.PolicyTagRelPO;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** MyBatis mapper for policy-tag relation metadata. */
public interface PolicyTagRelMapper {
  String POLICY_TAG_RELATION_TABLE_NAME = "policy_tag_relation_meta";

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
  @SelectProvider(type = PolicyTagRelSQLProviderFactory.class, method = "listPolicyPOsByTagId")
  List<PolicyPO> listPolicyPOsByTagId(@Param("tagId") Long tagId);

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
      type = PolicyTagRelSQLProviderFactory.class,
      method = "getPolicyPOByTagIdAndPolicyName")
  PolicyPO getPolicyPOByTagIdAndPolicyName(
      @Param("tagId") Long tagId, @Param("policyName") String policyName);

  @SelectProvider(type = PolicyTagRelSQLProviderFactory.class, method = "listTagPOsByPolicyId")
  List<TagPO> listTagPOsByPolicyId(@Param("policyId") Long policyId);

  @InsertProvider(type = PolicyTagRelSQLProviderFactory.class, method = "batchInsertPolicyTagRels")
  void batchInsertPolicyTagRels(@Param("policyTagRels") List<PolicyTagRelPO> policyTagRelPOs);

  @UpdateProvider(
      type = PolicyTagRelSQLProviderFactory.class,
      method = "batchDeletePolicyTagRelsByPolicyIdsAndTagId")
  void batchDeletePolicyTagRelsByPolicyIdsAndTagId(
      @Param("tagId") Long tagId, @Param("policyIds") List<Long> policyIds);

  @UpdateProvider(
      type = PolicyTagRelSQLProviderFactory.class,
      method = "softDeletePolicyTagRelsByMetalakeAndPolicyName")
  Integer softDeletePolicyTagRelsByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName);

  @UpdateProvider(
      type = PolicyTagRelSQLProviderFactory.class,
      method = "softDeletePolicyTagRelsByMetalakeAndTagName")
  Integer softDeletePolicyTagRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName);

  @UpdateProvider(
      type = PolicyTagRelSQLProviderFactory.class,
      method = "softDeletePolicyTagRelsByMetalakeId")
  void softDeletePolicyTagRelsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = PolicyTagRelSQLProviderFactory.class,
      method = "deletePolicyTagRelsByLegacyTimeline")
  Integer deletePolicyTagRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
