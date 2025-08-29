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

import static org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper.POLICY_META_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.PolicyVersionMapper.POLICY_VERSION_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.PolicyVersionBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.PolicyVersionPO;

public class PolicyVersionPostgreSQLProvider extends PolicyVersionBaseSQLProvider {
  @Override
  public String softDeletePolicyVersionByMetalakeAndPolicyName(
      String metalakeName, String policyName) {
    return "UPDATE "
        + POLICY_VERSION_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE metalake_id = (SELECT metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND policy_id = (SELECT policy_id FROM "
        + POLICY_META_TABLE_NAME
        + " pm WHERE pm.policy_name = #{policyName} AND pm.deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  @Override
  public String deletePolicyVersionsByLegacyTimeline(Long legacyTimeline, int limit) {
    return "DELETE FROM "
        + POLICY_VERSION_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + POLICY_VERSION_TABLE_NAME
        + " WHERE deleted_at = 0 AND legacy_timeline < #{legacyTimeline} LIMIT #{limit})";
  }

  @Override
  public String softDeletePolicyVersionsByRetentionLine(
      Long policyId, long versionRetentionLine, int limit) {
    return "UPDATE "
        + POLICY_VERSION_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE id IN (SELECT id FROM "
        + POLICY_VERSION_TABLE_NAME
        + " WHERE policy_id = #{policyId} AND version < #{versionRetentionLine}"
        + " AND deleted_at = 0 LIMIT #{limit})";
  }

  @Override
  public String softDeletePolicyVersionsByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + POLICY_VERSION_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String insertPolicyVersionOnDuplicateKeyUpdate(PolicyVersionPO policyVersion) {
    return "INSERT INTO "
        + POLICY_VERSION_TABLE_NAME
        + " (metalake_id, policy_id, version, policy_comment, enabled,"
        + " content, deleted_at)"
        + " VALUES ("
        + " #{policyVersion.metalakeId},"
        + " #{policyVersion.policyId},"
        + " #{policyVersion.version},"
        + " #{policyVersion.policyComment},"
        + " #{policyVersion.enabled},"
        + " #{policyVersion.content},"
        + " #{policyVersion.deletedAt})"
        + " ON CONFLICT (policy_id, version, deleted_at) DO UPDATE SET"
        + " policy_comment = #{policyVersion.policyComment},"
        + " enabled = #{policyVersion.enabled},"
        + " content = #{policyVersion.content},"
        + " deleted_at = #{policyVersion.deletedAt}";
  }
}
