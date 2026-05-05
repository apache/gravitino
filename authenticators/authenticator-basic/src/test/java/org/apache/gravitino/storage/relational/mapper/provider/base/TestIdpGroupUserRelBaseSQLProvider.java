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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpGroupUserRelBaseSQLProvider {

  protected IdpGroupUserRelBaseSQLProvider createProvider() {
    return new IdpGroupUserRelBaseSQLProvider();
  }

  protected String expectedDeleteAtClause() {
    return "deleted_at = #{deletedAt}";
  }

  @Test
  public void testSelectGroupNamesByUserId() {
    String normalizedSql =
        createProvider().selectGroupNamesByUserId(1L).replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("SELECT g.group_name"));
    Assertions.assertTrue(
        normalizedSql.contains("FROM idp_group_user_rel r JOIN idp_group_meta g"));
    Assertions.assertTrue(normalizedSql.contains("WHERE r.user_id = #{userId}"));
    Assertions.assertTrue(normalizedSql.contains("ORDER BY g.group_name"));
  }

  @Test
  public void testSelectUserNamesByGroupId() {
    String normalizedSql =
        createProvider().selectUserNamesByGroupId(1L).replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("SELECT u.user_name"));
    Assertions.assertTrue(normalizedSql.contains("FROM idp_group_user_rel r JOIN idp_user_meta u"));
    Assertions.assertTrue(normalizedSql.contains("WHERE r.group_id = #{groupId}"));
    Assertions.assertTrue(normalizedSql.contains("ORDER BY u.user_name"));
  }

  @Test
  public void testSelectRelatedUserIds() {
    String script = createProvider().selectRelatedUserIds(1L, Arrays.asList(10L, 20L));
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 1L);
    params.put("userIds", Arrays.asList(10L, 20L));

    String normalizedSql = renderScript(script, params);

    Assertions.assertTrue(normalizedSql.contains("SELECT user_id FROM idp_group_user_rel"));
    Assertions.assertTrue(normalizedSql.contains("WHERE group_id = ?"));
    Assertions.assertTrue(normalizedSql.matches(".*user_id IN \\( \\? , \\? \\).*"));
    Assertions.assertTrue(normalizedSql.contains("AND deleted_at = 0"));
  }

  @Test
  public void testSelectRelatedUserIdsWithEmptyUserIds() {
    String script = createProvider().selectRelatedUserIds(1L, Collections.emptyList());
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 1L);
    params.put("userIds", Collections.emptyList());

    String normalizedSql = renderScript(script, params);

    Assertions.assertFalse(
        normalizedSql.matches(".*\\bIN\\s*\\(\\s*\\).*"),
        "Empty userIds should not generate invalid SQL IN (...) with no values");
    Assertions.assertTrue(
        normalizedSql.matches(".*\\b1\\s*=\\s*0\\b.*"),
        "Empty userIds should result in an unsatisfiable WHERE clause (e.g., AND 1 = 0)");
  }

  @Test
  public void testSelectRelatedUserIdsWithNullUserIds() {
    String script = createProvider().selectRelatedUserIds(1L, null);
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 1L);

    String normalizedSql = renderScript(script, params);

    Assertions.assertFalse(
        normalizedSql.matches(".*\\bIN\\s*\\(\\s*\\).*"),
        "Null userIds should not generate invalid SQL IN (...) with no values");
    Assertions.assertTrue(
        normalizedSql.matches(".*\\b1\\s*=\\s*0\\b.*"),
        "Null userIds should result in an unsatisfiable WHERE clause (e.g., AND 1 = 0)");
  }

  @Test
  public void testBatchInsertLocalGroupUsers() {
    List<IdpGroupUserRelPO> relations =
        Arrays.asList(newRelation(1L, 10L, 20L), newRelation(2L, 10L, 21L));

    String script = createProvider().batchInsertLocalGroupUsers(relations);
    Map<String, Object> params = new HashMap<>();
    params.put("relations", relations);

    String normalizedSql = renderScript(script, params);

    Assertions.assertTrue(normalizedSql.contains("INSERT INTO idp_group_user_rel"));
    Assertions.assertTrue(
        normalizedSql.contains(
            "(id, group_id, user_id, audit_info, current_version, last_version, deleted_at)"));
    Assertions.assertTrue(
        normalizedSql.contains("VALUES"), "Batch insert SQL should include VALUES clause");
    Assertions.assertEquals(
        14,
        countOccurrences(normalizedSql, '?'),
        "Two group-user relations should render fourteen placeholders");
  }

  @Test
  public void testSoftDeleteLocalGroupUsers() {
    String script =
        createProvider().softDeleteLocalGroupUsers(10L, Arrays.asList(20L, 21L), 2L, "audit");
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 10L);
    params.put("userIds", Arrays.asList(20L, 21L));
    params.put("deletedAt", 2L);
    params.put("auditInfo", "audit");

    String normalizedSql = renderScript(script, params);

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_group_user_rel"));
    Assertions.assertTrue(
        normalizedSql.contains(expectedDeleteAtClause().replace("#{deletedAt}", "?")));
    Assertions.assertTrue(normalizedSql.contains("audit_info = ?"));
    Assertions.assertTrue(normalizedSql.contains("current_version = current_version + 1"));
    Assertions.assertTrue(normalizedSql.contains("last_version = last_version + 1"));
    Assertions.assertTrue(normalizedSql.matches(".*user_id IN \\( \\? , \\? \\).*"));
  }

  @Test
  public void testSoftDeleteLocalGroupUsersWithEmptyUserIds() {
    String script =
        createProvider().softDeleteLocalGroupUsers(10L, Collections.emptyList(), 2L, "audit");
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 10L);
    params.put("userIds", Collections.emptyList());
    params.put("deletedAt", 2L);
    params.put("auditInfo", "audit");

    String normalizedSql = renderScript(script, params);

    Assertions.assertFalse(
        normalizedSql.matches(".*\\bIN\\s*\\(\\s*\\).*"),
        "Empty userIds should not generate invalid SQL IN (...) with no values");
    Assertions.assertTrue(
        normalizedSql.matches(".*\\b1\\s*=\\s*0\\b.*"),
        "Empty userIds should result in an unsatisfiable WHERE clause (e.g., AND 1 = 0)");
  }

  @Test
  public void testSoftDeleteLocalGroupUsersWithNullUserIds() {
    String script = createProvider().softDeleteLocalGroupUsers(10L, null, 2L, "audit");
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 10L);
    params.put("deletedAt", 2L);
    params.put("auditInfo", "audit");

    String normalizedSql = renderScript(script, params);

    Assertions.assertFalse(
        normalizedSql.matches(".*\\bIN\\s*\\(\\s*\\).*"),
        "Null userIds should not generate invalid SQL IN (...) with no values");
    Assertions.assertTrue(
        normalizedSql.matches(".*\\b1\\s*=\\s*0\\b.*"),
        "Null userIds should result in an unsatisfiable WHERE clause (e.g., AND 1 = 0)");
  }

  @Test
  public void testSoftDeleteGroupUsersByUserId() {
    String normalizedSql =
        createProvider()
            .softDeleteGroupUsersByUserId(1L, 2L, "audit")
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_group_user_rel"));
    Assertions.assertTrue(normalizedSql.contains(expectedDeleteAtClause()));
    Assertions.assertTrue(normalizedSql.contains("audit_info = #{auditInfo}"));
    Assertions.assertTrue(normalizedSql.contains("WHERE user_id = #{userId} AND deleted_at = 0"));
  }

  @Test
  public void testSoftDeleteGroupUsersByGroupId() {
    String normalizedSql =
        createProvider()
            .softDeleteGroupUsersByGroupId(1L, 2L, "audit")
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_group_user_rel"));
    Assertions.assertTrue(normalizedSql.contains(expectedDeleteAtClause()));
    Assertions.assertTrue(normalizedSql.contains("audit_info = #{auditInfo}"));
    Assertions.assertTrue(normalizedSql.contains("WHERE group_id = #{groupId} AND deleted_at = 0"));
  }

  @Test
  public void testTruncateLocalGroupUserRel() {
    Assertions.assertEquals(
        "DELETE FROM idp_group_user_rel", createProvider().truncateLocalGroupUserRel());
  }

  private String renderScript(String script, Map<String, Object> params) {
    SqlSource sqlSource =
        new XMLLanguageDriver().createSqlSource(new Configuration(), script, Map.class);
    BoundSql boundSql = sqlSource.getBoundSql(params);
    return boundSql.getSql().replaceAll("\\s+", " ").trim();
  }

  private int countOccurrences(String input, char expectedChar) {
    int count = 0;
    for (int index = 0; index < input.length(); index++) {
      if (input.charAt(index) == expectedChar) {
        count++;
      }
    }
    return count;
  }

  private IdpGroupUserRelPO newRelation(Long id, Long groupId, Long userId) {
    return IdpGroupUserRelPO.builder()
        .withId(id)
        .withGroupId(groupId)
        .withUserId(userId)
        .withAuditInfo("audit")
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }
}
