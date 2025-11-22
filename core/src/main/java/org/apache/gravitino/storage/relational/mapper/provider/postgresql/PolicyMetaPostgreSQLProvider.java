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

import org.apache.gravitino.storage.relational.mapper.provider.base.PolicyMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.PolicyPO;

public class PolicyMetaPostgreSQLProvider extends PolicyMetaBaseSQLProvider {

  @Override
  public String softDeletePolicyByMetalakeAndPolicyName(String metalakeName, String policyName) {
    return "UPDATE "
        + POLICY_META_TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE metalake_id = (SELECT metalake_id FROM "
        + " metalake_meta mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND policy_name = #{policyName} AND deleted_at = 0";
  }

  @Override
  public String softDeletePolicyMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + POLICY_META_TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String deletePolicyMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return "DELETE FROM "
        + POLICY_META_TABLE_NAME
        + " WHERE policy_id IN (SELECT policy_id FROM "
        + POLICY_META_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }

  @Override
  public String insertPolicyMetaOnDuplicateKeyUpdate(PolicyPO policyPO) {
    return "INSERT INTO "
        + POLICY_META_TABLE_NAME
        + " (policy_id, policy_name, policy_type, metalake_id,"
        + " audit_info, current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{policyMeta.policyId},"
        + " #{policyMeta.policyName},"
        + " #{policyMeta.policyType},"
        + " #{policyMeta.metalakeId},"
        + " #{policyMeta.auditInfo},"
        + " #{policyMeta.currentVersion},"
        + " #{policyMeta.lastVersion},"
        + " #{policyMeta.deletedAt})"
        + " ON CONFLICT (policy_id) DO UPDATE SET"
        + " policy_name = #{policyMeta.policyName},"
        + " policy_type = #{policyMeta.policyType},"
        + " metalake_id = #{policyMeta.metalakeId},"
        + " audit_info = #{policyMeta.auditInfo},"
        + " current_version = #{policyMeta.currentVersion},"
        + " last_version = #{policyMeta.lastVersion},"
        + " deleted_at = #{policyMeta.deletedAt}";
  }
}
