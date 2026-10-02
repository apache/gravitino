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

import static org.apache.gravitino.storage.relational.mapper.PolicyVersionMapper.POLICY_VERSION_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.mapper.provider.base.PolicyVersionBaseSQLProvider;

public class PolicyVersionPostgreSQLProvider extends PolicyVersionBaseSQLProvider {
  @Override
  public String softDeletePolicyVersionsByPolicyId(Long policyId) {
    return "UPDATE "
        + POLICY_VERSION_TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE policy_id = #{policyId} AND deleted_at = 0";
  }

  @Override
  public String deletePolicyVersionsByLegacyTimeline(Long legacyTimeline, int limit) {
    return "DELETE FROM "
        + POLICY_VERSION_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + POLICY_VERSION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }

  @Override
  public String softDeletePolicyVersionsByRetentionLine(
      Long policyId, long versionRetentionLine, int limit) {
    return "UPDATE "
        + POLICY_VERSION_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE id IN (SELECT id FROM "
        + POLICY_VERSION_TABLE_NAME
        + " WHERE policy_id = #{policyId} AND version <= #{versionRetentionLine}"
        + " AND deleted_at = 0 LIMIT #{limit})";
  }

  @Override
  public String softDeletePolicyVersionsByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + POLICY_VERSION_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
