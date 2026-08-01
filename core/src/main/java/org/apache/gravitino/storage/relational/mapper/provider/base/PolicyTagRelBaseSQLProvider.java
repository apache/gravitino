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

import static org.apache.gravitino.storage.relational.mapper.PolicyTagRelMapper.POLICY_TAG_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.PolicyVersionMapper.POLICY_VERSION_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.po.PolicyTagRelPO;
import org.apache.ibatis.annotations.Param;

/** Base SQL provider for policy-tag relation metadata. */
public class PolicyTagRelBaseSQLProvider {

  public String listPolicyPOsByTagId(@Param("tagId") Long tagId) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version,"
        + " pm.deleted_at, pvi.id, pvi.metalake_id as version_metalake_id,"
        + " pvi.policy_id as version_policy_id, pvi.version, pvi.policy_comment,"
        + " pvi.enabled, pvi.content, pvi.deleted_at as version_deleted_at"
        + " FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm JOIN "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr ON pm.policy_id = ptr.policy_id"
        + " JOIN "
        + POLICY_VERSION_TABLE_NAME
        + " pvi ON pm.policy_id = pvi.policy_id AND pm.current_version = pvi.version"
        + " WHERE ptr.tag_id = #{tagId}"
        + " AND ptr.deleted_at = 0 AND pm.deleted_at = 0 AND pvi.deleted_at = 0";
  }

  public String getPolicyPOByTagIdAndPolicyName(
      @Param("tagId") Long tagId, @Param("policyName") String policyName) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version,"
        + " pm.deleted_at, pvi.id, pvi.metalake_id as version_metalake_id,"
        + " pvi.policy_id as version_policy_id, pvi.version, pvi.policy_comment,"
        + " pvi.enabled, pvi.content, pvi.deleted_at as version_deleted_at"
        + " FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm JOIN "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr ON pm.policy_id = ptr.policy_id"
        + " JOIN "
        + POLICY_VERSION_TABLE_NAME
        + " pvi ON pm.policy_id = pvi.policy_id AND pm.current_version = pvi.version"
        + " WHERE ptr.tag_id = #{tagId} AND pm.policy_name = #{policyName}"
        + " AND ptr.deleted_at = 0 AND pm.deleted_at = 0 AND pvi.deleted_at = 0";
  }

  public String listTagPOsByPolicyId(@Param("policyId") Long policyId) {
    return "SELECT tm.tag_id as tagId, tm.tag_name as tagName,"
        + " tm.metalake_id as metalakeId, tm.tag_comment as comment, tm.properties as properties,"
        + " tm.audit_info as auditInfo, tm.current_version as currentVersion,"
        + " tm.last_version as lastVersion, tm.deleted_at as deletedAt"
        + " FROM "
        + TagMetaMapper.TAG_TABLE_NAME
        + " tm JOIN "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr ON tm.tag_id = ptr.tag_id"
        + " WHERE ptr.policy_id = #{policyId}"
        + " AND ptr.deleted_at = 0 AND tm.deleted_at = 0";
  }

  public String batchInsertPolicyTagRels(
      @Param("policyTagRels") List<PolicyTagRelPO> policyTagRelPOs) {
    return "<script>"
        + "INSERT INTO "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " (policy_id, tag_id, audit_info, current_version, last_version, deleted_at)"
        + " VALUES "
        + "<foreach collection='policyTagRels' item='item' separator=','>"
        + "(#{item.policyId},"
        + " #{item.tagId},"
        + " #{item.auditInfo},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})"
        + "</foreach>"
        + "</script>";
  }

  public String batchDeletePolicyTagRelsByPolicyIdsAndTagId(
      @Param("tagId") Long tagId, @Param("policyIds") List<Long> policyIds) {
    return "<script>"
        + "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE policy_id IN "
        + "<foreach item='policyId' collection='policyIds' open='(' separator=',' close=')'>"
        + "#{policyId}"
        + "</foreach>"
        + " AND tag_id = #{tagId} AND deleted_at = 0"
        + "</script>";
  }

  public String softDeletePolicyTagRelsByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr SET ptr.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE ptr.policy_id IN (SELECT pm.policy_id FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm WHERE pm.metalake_id IN (SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND pm.policy_name = #{policyName} AND pm.deleted_at = 0) AND ptr.deleted_at = 0";
  }

  public String softDeletePolicyTagRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr SET ptr.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE ptr.tag_id IN (SELECT tm.tag_id FROM "
        + TagMetaMapper.TAG_TABLE_NAME
        + " tm WHERE tm.metalake_id IN (SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND tm.tag_name = #{tagName} AND tm.deleted_at = 0) AND ptr.deleted_at = 0";
  }

  public String softDeletePolicyTagRelsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr SET ptr.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE EXISTS (SELECT * FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm WHERE pm.metalake_id = #{metalakeId} AND pm.policy_id = ptr.policy_id"
        + " AND pm.deleted_at = 0) AND ptr.deleted_at = 0";
  }

  public String deletePolicyTagRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
