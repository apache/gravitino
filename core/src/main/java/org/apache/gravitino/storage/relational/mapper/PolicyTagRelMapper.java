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
import org.apache.gravitino.storage.relational.po.PolicyTagRelPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** MyBatis mapper for policy-to-tag relations. */
public interface PolicyTagRelMapper {
  /** The policy-to-tag relation table name. */
  String POLICY_TAG_RELATION_TABLE_NAME = "policy_tag_relation_meta";

  /**
   * Lists active policy-to-tag relations for the given tag names in a metalake.
   *
   * @param metalakeName The metalake name.
   * @param tagNames The tag names.
   * @return The active policy-to-tag relations.
   */
  @SelectProvider(type = PolicyTagRelSQLProviderFactory.class, method = "listByTagNames")
  List<PolicyTagRelPO> listByTagNames(
      @Param("metalakeName") String metalakeName, @Param("tagNames") List<String> tagNames);

  /**
   * Lists active policy-to-tag relations for the given policy names in a metalake.
   *
   * @param metalakeName The metalake name.
   * @param policyNames The policy names.
   * @return The active policy-to-tag relations.
   */
  @SelectProvider(type = PolicyTagRelSQLProviderFactory.class, method = "listByPolicyNames")
  List<PolicyTagRelPO> listByPolicyNames(
      @Param("metalakeName") String metalakeName, @Param("policyNames") List<String> policyNames);

  /**
   * Gets one active relation by policy ID and tag ID.
   *
   * @param policyId The policy ID.
   * @param tagId The tag ID.
   * @return The active relation, or null if no relation exists.
   */
  @SelectProvider(type = PolicyTagRelSQLProviderFactory.class, method = "getByPolicyIdAndTagId")
  PolicyTagRelPO getByPolicyIdAndTagId(
      @Param("policyId") Long policyId, @Param("tagId") Long tagId);

  /**
   * Inserts a policy-to-tag relation if the active pair does not exist.
   *
   * @param relation The relation to insert.
   * @return The number of inserted rows.
   */
  @InsertProvider(type = PolicyTagRelSQLProviderFactory.class, method = "insertIfAbsent")
  int insertIfAbsent(@Param("relation") PolicyTagRelPO relation);

  /**
   * Soft-deletes one active relation and advances its optimistic-concurrency version.
   *
   * @param relation The observed relation and version.
   * @return The number of updated rows.
   */
  @UpdateProvider(type = PolicyTagRelSQLProviderFactory.class, method = "softDeleteByIdAndVersion")
  int softDeleteByIdAndVersion(@Param("relation") PolicyTagRelPO relation);

  /**
   * Soft-deletes active relations in a metalake.
   *
   * @param metalakeId The metalake ID.
   * @return The number of updated rows.
   */
  @UpdateProvider(type = PolicyTagRelSQLProviderFactory.class, method = "softDeleteByMetalakeId")
  int softDeleteByMetalakeId(@Param("metalakeId") Long metalakeId);

  /**
   * Soft-deletes every active policy relation for a tag.
   *
   * @param tagId The tag ID.
   * @return The number of affected rows.
   */
  @UpdateProvider(type = PolicyTagRelSQLProviderFactory.class, method = "softDeleteByTagId")
  int softDeleteByTagId(@Param("tagId") Long tagId);

  /**
   * Physically deletes expired relation rows.
   *
   * @param legacyTimeline The exclusive deletion timestamp upper bound.
   * @param limit The maximum number of rows to delete.
   * @return The number of deleted rows.
   */
  @DeleteProvider(type = PolicyTagRelSQLProviderFactory.class, method = "deleteByLegacyTimeline")
  int deleteByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
