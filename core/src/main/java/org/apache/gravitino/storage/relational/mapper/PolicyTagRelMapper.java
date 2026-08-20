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

  @SelectProvider(type = PolicyTagRelSQLProviderFactory.class, method = "listByTagIds")
  List<PolicyTagRelPO> listByTagIds(@Param("tagIds") List<Long> tagIds);

  @SelectProvider(type = PolicyTagRelSQLProviderFactory.class, method = "listByPolicyIds")
  List<PolicyTagRelPO> listByPolicyIds(@Param("policyIds") List<Long> policyIds);

  @SelectProvider(type = PolicyTagRelSQLProviderFactory.class, method = "getByPolicyIdAndTagId")
  PolicyTagRelPO getByPolicyIdAndTagId(
      @Param("policyId") Long policyId, @Param("tagId") Long tagId);

  @InsertProvider(type = PolicyTagRelSQLProviderFactory.class, method = "insert")
  void insert(@Param("relation") PolicyTagRelPO relation);

  @UpdateProvider(type = PolicyTagRelSQLProviderFactory.class, method = "updateSelector")
  int updateSelector(@Param("relation") PolicyTagRelPO relation);

  @UpdateProvider(type = PolicyTagRelSQLProviderFactory.class, method = "softDeleteByPair")
  int softDeleteByPair(@Param("policyId") Long policyId, @Param("tagId") Long tagId);

  @UpdateProvider(
      type = PolicyTagRelSQLProviderFactory.class,
      method = "softDeleteByMetalakeAndPolicyName")
  int softDeleteByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName);

  @UpdateProvider(
      type = PolicyTagRelSQLProviderFactory.class,
      method = "softDeleteByMetalakeAndTagName")
  int softDeleteByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName);

  @UpdateProvider(type = PolicyTagRelSQLProviderFactory.class, method = "softDeleteByMetalakeId")
  int softDeleteByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(type = PolicyTagRelSQLProviderFactory.class, method = "deleteByLegacyTimeline")
  int deleteByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
