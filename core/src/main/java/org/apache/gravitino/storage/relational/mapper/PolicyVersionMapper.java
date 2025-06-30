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
import org.apache.gravitino.storage.relational.po.PolicyMaxVersionPO;
import org.apache.gravitino.storage.relational.po.PolicyVersionPO;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface PolicyVersionMapper {
  String POLICY_VERSION_TABLE_NAME = "policy_version_info";

  @InsertProvider(
      type = PolicyVersionSQLProviderFactory.class,
      method = "insertPolicyVersionOnDuplicateKeyUpdate")
  void insertPolicyVersionOnDuplicateKeyUpdate(
      @Param("policyVersion") PolicyVersionPO policyVersionPO);

  @InsertProvider(type = PolicyVersionSQLProviderFactory.class, method = "insertPolicyVersion")
  void insertPolicyVersion(@Param("policyVersion") PolicyVersionPO policyVersionPO);

  @UpdateProvider(
      type = PolicyVersionSQLProviderFactory.class,
      method = "softDeletePolicyVersionByMetalakeAndPolicyName")
  Integer softDeletePolicyVersionByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName);

  @UpdateProvider(
      type = PolicyVersionSQLProviderFactory.class,
      method = "deletePolicyVersionsByLegacyTimeline")
  Integer deletePolicyVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @SelectProvider(
      type = PolicyVersionSQLProviderFactory.class,
      method = "selectPolicyVersionsByRetentionCount")
  List<PolicyMaxVersionPO> selectPolicyVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount);

  @UpdateProvider(
      type = PolicyVersionSQLProviderFactory.class,
      method = "softDeletePolicyVersionsByRetentionLine")
  Integer softDeletePolicyVersionsByRetentionLine(
      @Param("policyId") Long policyId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit);

  @UpdateProvider(
      type = PolicyVersionSQLProviderFactory.class,
      method = "softDeletePolicyVersionsByMetalakeId")
  void softDeletePolicyVersionsByMetalakeId(@Param("metalakeId") Long metalakeId);
}
