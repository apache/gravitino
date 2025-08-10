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

import static org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper.POLICY_META_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.PolicyVersionMapper.POLICY_VERSION_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.PolicyVersionPO;
import org.apache.ibatis.annotations.Param;

public class PolicyVersionBaseSQLProvider {

  public String insertPolicyVersionOnDuplicateKeyUpdate(
      @Param("policyVersion") PolicyVersionPO policyVersion) {
    return "INSERT INTO "
        + POLICY_VERSION_TABLE_NAME
        + " (metalake_id, policy_id, version, policy_comment, enabled, content, deleted_at) "
        + "VALUES (#{policyVersion.metalakeId}, #{policyVersion.policyId}, #{policyVersion.version}, #{policyVersion.policyComment}, "
        + "#{policyVersion.enabled}, #{policyVersion.content}, #{policyVersion.deletedAt}) "
        + "ON DUPLICATE KEY UPDATE "
        + "metalake_id = #{policyVersion.metalakeId}, "
        + "policy_id = #{policyVersion.policyId}, "
        + "version = #{policyVersion.version}, "
        + "policy_comment = #{policyVersion.policyComment}, "
        + "enabled = #{policyVersion.enabled}, "
        + "content = #{policyVersion.content}, "
        + "deleted_at = #{policyVersion.deletedAt}";
  }

  public String insertPolicyVersion(@Param("policyVersion") PolicyVersionPO policyVersion) {
    return "INSERT INTO "
        + POLICY_VERSION_TABLE_NAME
        + " (metalake_id, policy_id, version, policy_comment, enabled, content, deleted_at) "
        + "VALUES (#{policyVersion.metalakeId}, #{policyVersion.policyId}, #{policyVersion.version}, "
        + "#{policyVersion.policyComment}, #{policyVersion.enabled}, #{policyVersion.content}, "
        + "#{policyVersion.deletedAt})";
  }

  public String softDeletePolicyVersionByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return "UPDATE "
        + POLICY_VERSION_TABLE_NAME
        + " pv SET pv.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE pv.metalake_id IN ("
        + " SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND pv.policy_id IN ("
        + " SELECT pm.policy_id FROM "
        + POLICY_META_TABLE_NAME
        + " pm WHERE pm.policy_name = #{policyName} AND pm.deleted_at = 0"
        + " AND pm.metalake_id IN ("
        + " SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0))"
        + " AND pv.deleted_at = 0";
  }

  public String deletePolicyVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + POLICY_VERSION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  public String selectPolicyVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount) {
    return "SELECT policy_id as policyId, "
        + "Max(version) as version "
        + "FROM "
        + POLICY_VERSION_TABLE_NAME
        + " WHERE version > #{versionRetentionCount} AND deleted_at = 0 "
        + "GROUP BY policy_id";
  }

  public String softDeletePolicyVersionsByRetentionLine(
      @Param("policyId") Long policyId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return "UPDATE "
        + POLICY_VERSION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE policy_id = #{policyId} AND version <= #{versionRetentionLine} AND deleted_at = 0"
        + " LIMIT #{limit}";
  }

  public String softDeletePolicyVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + POLICY_VERSION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
