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

import static org.apache.gravitino.storage.relational.mapper.PolicyTagRelMapper.POLICY_TAG_RELATION_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.PolicyTagRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

/** PostgreSQL SQL provider for policy-tag relation metadata. */
public class PolicyTagRelPostgreSQLProvider extends PolicyTagRelBaseSQLProvider {
  private static final String DELETED_AT_NOW_EXPRESSION =
      " CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)";

  @Override
  public String batchDeletePolicyTagRelsByPolicyIdsAndTagId(Long tagId, List<Long> policyIds) {
    return "<script>"
        + "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " WHERE policy_id IN "
        + "<foreach item='policyId' collection='policyIds' open='(' separator=',' close=')'>"
        + "#{policyId}"
        + "</foreach>"
        + " AND tag_id = #{tagId} AND deleted_at = 0"
        + "</script>";
  }

  @Override
  public String softDeletePolicyTagRelsByMetalakeAndPolicyName(
      String metalakeName, String policyName) {
    return "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " WHERE ptr.policy_id IN (SELECT pm.policy_id FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm WHERE pm.metalake_id IN (SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND pm.policy_name = #{policyName} AND pm.deleted_at = 0) AND ptr.deleted_at = 0";
  }

  @Override
  public String softDeletePolicyTagRelsByMetalakeAndTagName(String metalakeName, String tagName) {
    return "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " WHERE ptr.tag_id IN (SELECT tm.tag_id FROM "
        + TagMetaMapper.TAG_TABLE_NAME
        + " tm WHERE tm.metalake_id IN (SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND tm.tag_name = #{tagName} AND tm.deleted_at = 0) AND ptr.deleted_at = 0";
  }

  @Override
  public String softDeletePolicyTagRelsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr SET deleted_at ="
        + DELETED_AT_NOW_EXPRESSION
        + " WHERE EXISTS (SELECT * FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm WHERE pm.metalake_id = #{metalakeId} AND pm.policy_id = ptr.policy_id"
        + " AND pm.deleted_at = 0) AND ptr.deleted_at = 0";
  }

  @Override
  public String deletePolicyTagRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
