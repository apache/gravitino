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
import static org.apache.gravitino.storage.relational.mapper.PolicyTagRelMapper.POLICY_TAG_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.PolicyVersionMapper.POLICY_VERSION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.TagMetaMapper.TAG_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.ibatis.annotations.Param;

public class PolicyMetaBaseSQLProvider {

  public String listPolicyPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version, pm.occ_version,"
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
        + " pm.audit_info, pm.current_version, pm.last_version, pm.occ_version,"
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

  public String insertPolicyMeta(@Param("policyMeta") PolicyPO policyPO) {
    return "INSERT INTO "
        + POLICY_META_TABLE_NAME
        + " (policy_id, policy_name, policy_type, metalake_id,"
        + " audit_info, current_version, last_version, occ_version, deleted_at)"
        + " VALUES (#{policyMeta.policyId}, #{policyMeta.policyName}, #{policyMeta.policyType},"
        + " #{policyMeta.metalakeId}, #{policyMeta.auditInfo}, #{policyMeta.currentVersion},"
        + " #{policyMeta.lastVersion}, #{policyMeta.occVersion}, #{policyMeta.deletedAt})";
  }

  public String selectPolicyMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version, pm.occ_version,"
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
        + " occ_version = #{newPolicyMeta.occVersion},"
        + " deleted_at = #{newPolicyMeta.deletedAt}"
        + " WHERE policy_id = #{oldPolicyMeta.policyId}"
        + " AND occ_version = #{oldPolicyMeta.occVersion}"
        + " AND deleted_at = 0";
  }

  /** Returns SQL that soft-deletes a policy using its stable ID and observed OCC version. */
  public String softDeletePolicyByIdAndVersion(
      @Param("policyId") Long policyId, @Param("occVersion") Long occVersion) {
    return "UPDATE "
        + POLICY_META_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE policy_id = #{policyId} AND occ_version = #{occVersion}"
        + " AND deleted_at = 0";
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
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  /**
   * One join of policy, metalake, and the current policy version, selected by policy id.
   *
   * @param policyId policy id
   * @return the select statement
   */
  public String selectPolicyMetaByPolicyId(@Param("policyId") Long policyId) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version, pm.occ_version,"
        + " pm.deleted_at, mm.metalake_name as metalake_name,"
        + " pvi.id, pvi.metalake_id as version_metalake_id, pvi.policy_id as version_policy_id,"
        + " pvi.version, pvi.policy_comment, pvi.enabled, pvi.content,"
        + " pvi.deleted_at as version_deleted_at"
        + " FROM "
        + POLICY_META_TABLE_NAME
        + " pm JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON pm.metalake_id = mm.metalake_id"
        + " JOIN "
        + POLICY_VERSION_TABLE_NAME
        + " pvi ON pm.policy_id = pvi.policy_id"
        + " AND pm.current_version = pvi.version"
        + " WHERE pm.policy_id = #{policyId}"
        + " AND pm.deleted_at = 0 AND mm.deleted_at = 0"
        + " AND pvi.deleted_at = 0";
  }

  public String selectPolicyByPolicyId(@Param("policyId") Long policyId) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version, pm.occ_version,"
        + " pm.deleted_at"
        + " FROM "
        + POLICY_META_TABLE_NAME
        + " pm "
        + " WHERE"
        + " pm.policy_id = #{policyId}"
        + " AND pm.deleted_at = 0 ";
  }

  /** Returns SQL that selects and exclusively locks an active policy by ID. */
  public String selectPolicyByPolicyIdForUpdate(@Param("policyId") Long policyId) {
    return selectPolicyByPolicyId(policyId) + " FOR UPDATE";
  }

  /**
   * Returns SQL that selects and exclusively locks several active policies, ordered by policy ID so
   * that concurrent callers take the row locks in the same order.
   */
  public String listPolicyPOsByPolicyIdsForUpdate(@Param("policyIds") List<Long> policyIds) {
    return "<script>"
        + selectPolicyPOsByPolicyIdsBody()
        + " ORDER BY pm.policy_id FOR UPDATE</script>";
  }

  public String listPolicyPOsByPolicyIds(@Param("policyIds") List<Long> policyIds) {
    return "<script>" + selectPolicyPOsByPolicyIdsBody() + "</script>";
  }

  public String selectPolicyMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("policyName") String policyName) {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version, pm.occ_version,"
        + " pm.deleted_at"
        + " FROM "
        + POLICY_META_TABLE_NAME
        + " pm "
        + " WHERE"
        + " pm.metalake_id = #{metalakeId}"
        + " AND pm.policy_name = #{policyName}"
        + " AND pm.deleted_at = 0 ";
  }

  /** Returns SQL that selects and exclusively locks an active policy by its natural key. */
  public String selectPolicyMetaByMetalakeIdAndNameForUpdate(
      @Param("metalakeId") Long metalakeId, @Param("policyName") String policyName) {
    return selectPolicyMetaByMetalakeIdAndName(metalakeId, policyName) + " FOR UPDATE";
  }

  public String batchSelectPolicyByIdentifier(
      @Param("metalakeName") String metalakeName, @Param("policyNames") List<String> policyNames) {
    return "<script>"
        + "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version, pm.occ_version, pm.deleted_at,"
        + " pv.id, pv.metalake_id as version_metalake_id, pv.policy_id as version_policy_id,"
        + " pv.version, pv.policy_comment, pv.enabled, pv.content, pv.deleted_at as version_deleted_at"
        + " FROM "
        + POLICY_META_TABLE_NAME
        + " pm"
        + " INNER JOIN "
        + POLICY_VERSION_TABLE_NAME
        + " pv ON pm.policy_id = pv.policy_id AND pm.current_version = pv.version"
        + " JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON pm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName}"
        + " AND pm.policy_name IN ("
        + "<foreach collection='policyNames' item='policyName' separator=','>"
        + "#{policyName}"
        + "</foreach>"
        + " )"
        + " AND pm.deleted_at = 0 AND pv.deleted_at = 0 AND mm.deleted_at = 0"
        + "</script>";
  }

  /**
   * Returns the shared body of the by-ID list queries, without the enclosing {@code <script>} tag,
   * so the plain and the locking variant cannot drift apart when the selected columns change.
   */
  private String selectPolicyPOsByPolicyIdsBody() {
    return "SELECT pm.policy_id, pm.policy_name, pm.policy_type, pm.metalake_id,"
        + " pm.audit_info, pm.current_version, pm.last_version, pm.occ_version,"
        + " pm.deleted_at"
        + " FROM "
        + POLICY_META_TABLE_NAME
        + " pm"
        + " WHERE pm.deleted_at = 0"
        + " AND pm.policy_id IN ("
        + "<foreach collection='policyIds' item='policyId' separator=','>"
        + "#{policyId}"
        + "</foreach>"
        + ")";
  }

  /**
   * Lists, for each table, how many distinct enabled policies of {@code policyType} match on the
   * table's nearest level and the smallest of their ids, in one statement.
   *
   * <p>A table inherits from its schema, the hierarchical ancestors of that schema, and its
   * catalog. For each tag the nearest assignment wins, including its values. A policy matches when
   * its selector is null, {@code ALL_VALUES}, or the {@code TAG_VALUE} JSON of one effective value;
   * tag and selector values never contain characters the selector JSON escapes. The statement uses
   * derived tables instead of CTEs and window functions so that MySQL 5.7 can run it.
   *
   * @param tableIds table ids
   * @param policyType {@link org.apache.gravitino.policy.Policy.BuiltInType#policyType()} wire
   *     value
   * @param schemaSeparator physical hierarchical schema separator
   * @return SQL
   */
  public String listNearestPolicyIds(
      @Param("tableIds") List<Long> tableIds,
      @Param("policyType") String policyType,
      @Param("schemaSeparator") String schemaSeparator) {
    String matches = nearestPolicyMatches();
    return "<script>"
        + "SELECT m.table_id AS tableId,"
        + " COUNT(DISTINCT m.policy_id) AS policyCount,"
        + " MIN(m.policy_id) AS policyId"
        + " FROM ("
        + matches
        + ") m"
        + " JOIN (SELECT n.table_id, MIN(n.distance) AS distance FROM ("
        + matches
        + ") n GROUP BY n.table_id) nearest"
        + " ON nearest.table_id = m.table_id AND nearest.distance = m.distance"
        + " GROUP BY m.table_id"
        + "</script>";
  }

  /**
   * Enabled policies matched through each table's effective tags: table_id, distance, policy_id.
   */
  private static String nearestPolicyMatches() {
    String assignments = ancestorTagAssignments();
    return "SELECT a.table_id, a.distance, pm.policy_id"
        + " FROM ("
        + assignments
        + ") a"
        + " JOIN (SELECT e.table_id, e.tag_id, MIN(e.distance) AS distance FROM ("
        + assignments
        + ") e GROUP BY e.table_id, e.tag_id) effective"
        + " ON effective.table_id = a.table_id AND effective.tag_id = a.tag_id"
        + " AND effective.distance = a.distance"
        + " JOIN "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr ON ptr.tag_id = a.tag_id AND ptr.deleted_at = 0"
        + " JOIN "
        + POLICY_META_TABLE_NAME
        + " pm ON pm.policy_id = ptr.policy_id AND pm.deleted_at = 0"
        + " JOIN "
        + POLICY_VERSION_TABLE_NAME
        + " pvi ON pvi.policy_id = pm.policy_id AND pvi.version = pm.current_version"
        + " AND pvi.deleted_at = 0"
        + " WHERE pm.policy_type = #{policyType} AND pvi.enabled = TRUE"
        + " AND (ptr.selector IS NULL"
        + " OR ptr.selector = '{\"type\":\"ALL_VALUES\"}'"
        + " OR ptr.selector = CONCAT('{\"type\":\"TAG_VALUE\",\"value\":\"', a.tag_value, '\"}'))";
  }

  /** Tag assignments on each table and its ancestors: table_id, distance, tag_id, tag_value. */
  private static String ancestorTagAssignments() {
    return "SELECT lv.table_id, lv.distance, tr.tag_id, tr.tag_value"
        + " FROM ("
        + tableAncestors()
        + ") lv"
        + " JOIN "
        + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " tr ON tr.metadata_object_id = lv.object_id"
        + " AND tr.metadata_object_type = lv.object_type AND tr.deleted_at = 0"
        + " JOIN "
        + TAG_TABLE_NAME
        + " tg ON tg.tag_id = tr.tag_id AND tg.deleted_at = 0";
  }

  /**
   * Each table and its ancestors with a distance that grows outward: the table is 0, a schema is 1
   * plus how much shorter its name is than the table's schema, and the catalog is farther than any
   * schema.
   */
  private static String tableAncestors() {
    String requestedTables =
        " WHERE tm.deleted_at = 0 AND tm.table_id IN ("
            + "<foreach collection='tableIds' item='tableId' separator=','>#{tableId}</foreach>)";
    String leafSchema =
        " JOIN "
            + SchemaMetaMapper.TABLE_NAME
            + " leaf ON leaf.schema_id = tm.schema_id AND leaf.deleted_at = 0";
    return "SELECT tm.table_id, 'TABLE' AS object_type, tm.table_id AS object_id, 0 AS distance"
        + " FROM "
        + TableMetaMapper.TABLE_NAME
        + " tm"
        + requestedTables
        + " UNION ALL"
        + " SELECT tm.table_id, 'SCHEMA', anc.schema_id,"
        + " 1 + CHAR_LENGTH(leaf.schema_name) - CHAR_LENGTH(anc.schema_name)"
        + " FROM "
        + TableMetaMapper.TABLE_NAME
        + " tm"
        + leafSchema
        + " JOIN "
        + SchemaMetaMapper.TABLE_NAME
        + " anc ON anc.catalog_id = leaf.catalog_id AND anc.deleted_at = 0"
        + " AND (anc.schema_id = leaf.schema_id"
        + " OR (CHAR_LENGTH(leaf.schema_name) > CHAR_LENGTH(anc.schema_name)"
        + " AND LEFT(leaf.schema_name, CHAR_LENGTH(anc.schema_name)) = anc.schema_name"
        + " AND SUBSTRING(leaf.schema_name, CHAR_LENGTH(anc.schema_name) + 1, 1)"
        + " = #{schemaSeparator}))"
        + requestedTables
        + " UNION ALL"
        + " SELECT tm.table_id, 'CATALOG', tm.catalog_id, 1 + CHAR_LENGTH(leaf.schema_name)"
        + " FROM "
        + TableMetaMapper.TABLE_NAME
        + " tm"
        + leafSchema
        + requestedTables;
  }
}
