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
import org.apache.gravitino.storage.relational.po.PolicyRoleRelPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** MyBatis mapper for the policy-to-role references derived from policy content. */
public interface PolicyRoleRelMapper {
  /** The policy-to-role reference table name. */
  String POLICY_ROLE_RELATION_TABLE_NAME = "policy_role_relation_meta";

  /**
   * Lists the active role references of one policy.
   *
   * @param policyId The policy ID.
   * @return The active references, ordered by role name.
   */
  @SelectProvider(type = PolicyRoleRelSQLProviderFactory.class, method = "listByPolicyId")
  List<PolicyRoleRelPO> listByPolicyId(@Param("policyId") Long policyId);

  /**
   * Lists the active references to one role name in a metalake.
   *
   * @param metalakeName The metalake name.
   * @param roleName The role name.
   * @return The active references, ordered by policy name.
   */
  @SelectProvider(type = PolicyRoleRelSQLProviderFactory.class, method = "listByRoleName")
  List<PolicyRoleRelPO> listByRoleName(
      @Param("metalakeName") String metalakeName, @Param("roleName") String roleName);

  /**
   * Inserts a reference.
   *
   * @param relation The reference to insert.
   * @return The number of inserted rows.
   */
  @InsertProvider(type = PolicyRoleRelSQLProviderFactory.class, method = "insert")
  int insert(@Param("relation") PolicyRoleRelPO relation);

  /**
   * Soft-deletes one active reference and advances its optimistic-concurrency version.
   *
   * @param relation The observed reference and version.
   * @return The number of updated rows.
   */
  @UpdateProvider(type = PolicyRoleRelSQLProviderFactory.class, method = "softDeleteByIdAndVersion")
  int softDeleteByIdAndVersion(@Param("relation") PolicyRoleRelPO relation);

  /**
   * Soft-deletes every active reference of a policy.
   *
   * @param policyId The policy ID.
   * @return The number of affected rows.
   */
  @UpdateProvider(type = PolicyRoleRelSQLProviderFactory.class, method = "softDeleteByPolicyId")
  int softDeleteByPolicyId(@Param("policyId") Long policyId);

  /**
   * Soft-deletes active references in a metalake.
   *
   * @param metalakeId The metalake ID.
   * @return The number of affected rows.
   */
  @UpdateProvider(type = PolicyRoleRelSQLProviderFactory.class, method = "softDeleteByMetalakeId")
  int softDeleteByMetalakeId(@Param("metalakeId") Long metalakeId);

  /**
   * Physically deletes expired reference rows.
   *
   * @param legacyTimeline The exclusive deletion timestamp upper bound.
   * @param limit The maximum number of rows to delete.
   * @return The number of deleted rows.
   */
  @DeleteProvider(type = PolicyRoleRelSQLProviderFactory.class, method = "deleteByLegacyTimeline")
  int deleteByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
