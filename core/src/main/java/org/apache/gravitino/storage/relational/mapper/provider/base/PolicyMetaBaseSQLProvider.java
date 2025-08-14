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

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.ibatis.annotations.Param;

public class PolicyMetaBaseSQLProvider {

  public String listPolicyPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version,"
        + " pm.deleted_at, pvi.id, pvi.metalake_id as version_metalake_id, pvi.policy_id as version_policy_id,"
        + " pvi.version, pvi.policy_comment, pvi.enabled, pvi.content, pvi.deleted_at as version_deleted_at"
        + " FROM "
        + POLICY_META_TABLE_NAME
        + " pm JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON pm.metalake_id = mm.metalake_id"
        + " JOIN "
        + POLICY_VERSION_TABLE_NAME
        + " pvi ON pm.policy_id = pvi.policy_id"
        + " AND pm.current_version = pvi.version"
        + " WHERE mm.metalake_name = #{metalakeName}"
        + " AND pm.deleted_at = 0 AND mm.deleted_at = 0"
        + " AND pvi.deleted_at = 0";
  }

  public String listPolicyPOsByMetalakeAndPolicyNames(
      @Param("metalakeName") String metalakeName, @Param("policyNames") List<String> policyNames) {
    return "<script>"
        + "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version,"
        + " pm.deleted_at, pvi.id, pvi.metalake_id as version_metalake_id, pvi.policy_id as version_policy_id,"
        + " pvi.version, pvi.policy_comment, pvi.enabled, pvi.content, pvi.deleted_at as version_deleted_at"
        + " FROM "
        + POLICY_META_TABLE_NAME
        + " pm JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON pm.metalake_id = mm.metalake_id"
        + " JOIN "
        + POLICY_VERSION_TABLE_NAME
        + " pvi ON pm.policy_id = pvi.policy_id"
        + " AND pm.current_version = pvi.version"
        + " WHERE mm.metalake_name = #{metalakeName}"
        + " AND pm.policy_name IN "
        + " <foreach item='policyName' index='index' collection='policyNames' open='(' separator=',' close=')'>"
        + " #{policyName}"
        + " </foreach>"
        + " AND pm.deleted_at = 0 AND mm.deleted_at = 0"
        + " AND pvi.deleted_at = 0"
        + "</script>";
  }

  public String insertPolicyMetaOnDuplicateKeyUpdate(@Param("policyMeta") PolicyPO policyPO) {
    return "INSERT INTO "
        + POLICY_META_TABLE_NAME
        + " (policy_id, policy_name, policy_type, metalake_id,"
        + " audit_info, current_version, last_version, deleted_at)"
        + " VALUES (#{policyMeta.policyId}, #{policyMeta.policyName}, #{policyMeta.policyType},"
        + " #{policyMeta.metalakeId}, #{policyMeta.auditInfo}, #{policyMeta.currentVersion},"
        + " #{policyMeta.lastVersion}, #{policyMeta.deletedAt})"
        + " ON DUPLICATE KEY UPDATE"
        + " policy_name = #{policyMeta.policyName},"
        + " policy_type = #{policyMeta.policyType},"
        + " metalake_id = #{policyMeta.metalakeId},"
        + " audit_info = #{policyMeta.auditInfo},"
        + " current_version = #{policyMeta.currentVersion},"
        + " last_version = #{policyMeta.lastVersion},"
        + " deleted_at = #{policyMeta.deletedAt}";
  }

  public String insertPolicyMeta(@Param("policyMeta") PolicyPO policyPO) {
    return "INSERT INTO "
        + POLICY_META_TABLE_NAME
        + " (policy_id, policy_name, policy_type, metalake_id,"
        + " audit_info, current_version, last_version, deleted_at)"
        + " VALUES (#{policyMeta.policyId}, #{policyMeta.policyName}, #{policyMeta.policyType},"
        + " #{policyMeta.metalakeId}, #{policyMeta.auditInfo}, #{policyMeta.currentVersion},"
        + " #{policyMeta.lastVersion}, #{policyMeta.deletedAt})";
  }

  public String selectPolicyMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version,"
        + " pm.deleted_at, pvi.id, pvi.metalake_id as version_metalake_id, pvi.policy_id as version_policy_id,"
        + " pvi.version, pvi.policy_comment, pvi.enabled, pvi.content, pvi.deleted_at as version_deleted_at"
        + " FROM "
        + POLICY_META_TABLE_NAME
        + " pm JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON pm.metalake_id = mm.metalake_id"
        + " JOIN "
        + POLICY_VERSION_TABLE_NAME
        + " pvi ON pm.policy_id = pvi.policy_id"
        + " AND pm.current_version = pvi.version"
        + " WHERE mm.metalake_name = #{metalakeName}"
        + " AND pm.policy_name = #{policyName}"
        + " AND pm.deleted_at = 0 AND mm.deleted_at = 0"
        + " AND pvi.deleted_at = 0";
  }

  public String updatePolicyMeta(
      @Param("newPolicyMeta") PolicyPO newPolicyMeta,
      @Param("oldPolicyMeta") PolicyPO oldPolicyMeta) {
    return "UPDATE "
        + POLICY_META_TABLE_NAME
        + " SET policy_name = #{newPolicyMeta.policyName},"
        + " policy_type = #{newPolicyMeta.policyType},"
        + " metalake_id = #{newPolicyMeta.metalakeId},"
        + " audit_info = #{newPolicyMeta.auditInfo},"
        + " current_version = #{newPolicyMeta.currentVersion},"
        + " last_version = #{newPolicyMeta.lastVersion},"
        + " deleted_at = #{newPolicyMeta.deletedAt}"
        + " WHERE policy_id = #{oldPolicyMeta.policyId}"
        + " AND policy_name = #{oldPolicyMeta.policyName}"
        + " AND policy_type = #{oldPolicyMeta.policyType}"
        + " AND metalake_id = #{oldPolicyMeta.metalakeId}"
        + " AND audit_info = #{oldPolicyMeta.auditInfo}"
        + " AND current_version = #{oldPolicyMeta.currentVersion}"
        + " AND last_version = #{oldPolicyMeta.lastVersion}"
        + " AND deleted_at = 0";
  }

  public String softDeletePolicyByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return "UPDATE "
        + POLICY_META_TABLE_NAME
        + " pm SET pm.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE pm.metalake_id IN ("
        + " SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND pm.policy_name = #{policyName} AND pm.deleted_at = 0";
  }

  public String deletePolicyMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + POLICY_META_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  public String softDeletePolicyMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + POLICY_META_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
