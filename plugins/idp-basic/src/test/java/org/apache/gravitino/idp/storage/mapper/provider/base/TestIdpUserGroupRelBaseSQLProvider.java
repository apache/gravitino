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

package org.apache.gravitino.idp.storage.mapper.provider.base;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserGroupRelBaseSQLProvider {

  protected IdpUserGroupRelBaseSQLProvider createProvider() {
    return new IdpUserGroupRelBaseSQLProvider() {
      @Override
      protected String currentTimeMillisExpression() {
        return "CURRENT_TIME_MILLIS()";
      }
    };
  }

  protected String expectedDeleteAtClause() {
    return "deleted_at = CURRENT_TIME_MILLIS()";
  }

  protected String expectedDeleteIdpUserGroupRelMetasByLegacyTimelineSql() {
    return "DELETE FROM idp_user_group_rel WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline}"
        + " LIMIT #{limit}";
  }

  @Test
  void testSelectGroupNamesByUsername() {
    String normalizedSql =
        createProvider().selectGroupNamesByUsername("alice").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("SELECT g.group_name"));
    Assertions.assertTrue(normalizedSql.contains("FROM idp_user_meta u JOIN idp_user_group_rel r"));
    Assertions.assertTrue(normalizedSql.contains("JOIN idp_group_meta g"));
    Assertions.assertTrue(normalizedSql.contains("r.user_id = u.user_id AND r.deleted_at = 0"));
    Assertions.assertTrue(normalizedSql.contains("WHERE u.user_name = #{username}"));
    Assertions.assertTrue(normalizedSql.contains("ORDER BY g.group_name"));
  }

  @Test
  void testSelectUsernamesByGroupName() {
    String normalizedSql =
        createProvider().selectUsernamesByGroupName("dev").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("SELECT u.user_name"));
    Assertions.assertTrue(
        normalizedSql.contains("FROM idp_group_meta g JOIN idp_user_group_rel r"));
    Assertions.assertTrue(normalizedSql.contains("JOIN idp_user_meta u"));
    Assertions.assertTrue(normalizedSql.contains("r.group_id = g.group_id AND r.deleted_at = 0"));
    Assertions.assertTrue(normalizedSql.contains("WHERE g.group_name = #{groupName}"));
    Assertions.assertTrue(normalizedSql.contains("ORDER BY u.user_name"));
  }

  @Test
  void testBatchInsertIdpUserGroups() {
    List<IdpUserGroupRelPO> relations =
        Arrays.asList(newRelation(1L, 20L, 10L), newRelation(2L, 21L, 10L));

    String script = createProvider().batchInsertRelations(relations);
    Map<String, Object> params = new HashMap<>();
    params.put("relations", relations);

    String normalizedSql = renderScript(script, params);

    Assertions.assertTrue(normalizedSql.contains("INSERT INTO idp_user_group_rel"));
    Assertions.assertTrue(
        normalizedSql.contains(
            "(id, user_id, group_id, current_version, last_version, deleted_at)"));
    Assertions.assertTrue(
        normalizedSql.contains("VALUES"), "Batch insert SQL should include VALUES clause");
    Assertions.assertEquals(
        12,
        countOccurrences(normalizedSql, '?'),
        "Two user-group relations should render twelve placeholders");
  }

  @Test
  void testSoftDeleteIdpUserGroups() {
    String script = createProvider().softDeleteRelations(10L, Arrays.asList(20L, 21L));
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 10L);
    params.put("userIds", Arrays.asList(20L, 21L));

    String normalizedSql = renderScript(script, params);

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_user_group_rel"));
    Assertions.assertTrue(normalizedSql.contains(expectedDeleteAtClause()));
    Assertions.assertTrue(normalizedSql.matches(".*user_id IN \\( \\? , \\? \\).*"));
  }

  @Test
  void testSoftDeleteIdpUserGroupsWithEmptyUserIds() {
    String script = createProvider().softDeleteRelations(10L, Collections.emptyList());
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 10L);
    params.put("userIds", Collections.emptyList());

    String normalizedSql = renderScript(script, params);

    Assertions.assertFalse(
        normalizedSql.matches(".*\\bIN\\s*\\(\\s*\\).*"),
        "Empty userIds should not generate invalid SQL IN (...) with no values");
    Assertions.assertFalse(normalizedSql.matches(".*\\b1\\s*=\\s*0\\b.*"));
    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_user_group_rel"));
    Assertions.assertTrue(normalizedSql.contains(expectedDeleteAtClause()));
    Assertions.assertTrue(normalizedSql.matches(".*WHERE group_id = \\? AND deleted_at = 0.*"));
  }

  @Test
  void testSoftDeleteIdpUserGroupsWithNullUserIds() {
    String script = createProvider().softDeleteRelations(10L, null);
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 10L);

    Assertions.assertThrows(BuilderException.class, () -> renderScript(script, params));
  }

  @Test
  void testSoftDeleteRelationsByUsername() {
    String normalizedSql =
        createProvider().softDeleteRelationsByUsername("alice").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_user_group_rel"));
    Assertions.assertTrue(normalizedSql.contains("INNER JOIN idp_user_meta u"));
    Assertions.assertTrue(normalizedSql.contains("u.user_id = r.user_id AND u.deleted_at = 0"));
    Assertions.assertTrue(normalizedSql.contains(expectedDeleteAtClause()));
    Assertions.assertTrue(normalizedSql.contains("WHERE u.user_name = #{username}"));
    Assertions.assertTrue(normalizedSql.contains("AND r.deleted_at = 0"));
  }

  @Test
  void testSoftDeleteRelationsByGroupName() {
    String normalizedSql =
        createProvider().softDeleteRelationsByGroupName("dev").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_user_group_rel"));
    Assertions.assertTrue(normalizedSql.contains("INNER JOIN idp_group_meta g"));
    Assertions.assertTrue(normalizedSql.contains("g.group_id = r.group_id AND g.deleted_at = 0"));
    Assertions.assertTrue(normalizedSql.contains(expectedDeleteAtClause()));
    Assertions.assertTrue(normalizedSql.contains("WHERE g.group_name = #{groupName}"));
    Assertions.assertTrue(normalizedSql.contains("AND r.deleted_at = 0"));
  }

  @Test
  void testDeleteIdpUserGroupRelMetasByLegacyTimeline() {
    String normalizedSql =
        createProvider()
            .deleteIdpUserGroupRelMetasByLegacyTimeline(1L, 2)
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(expectedDeleteIdpUserGroupRelMetasByLegacyTimelineSql(), normalizedSql);
  }

  @Test
  void testCurrentTimeMillisExpression() {
    Assertions.assertEquals(
        "(UNIX_TIMESTAMP() * 1000.0)",
        new IdpUserGroupRelBaseSQLProvider().currentTimeMillisExpression());
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

  private IdpUserGroupRelPO newRelation(Long id, Long userId, Long groupId) {
    return IdpUserGroupRelPO.builder()
        .withId(id)
        .withUserId(userId)
        .withGroupId(groupId)
        .withCurrentVersion(1L)
        .withLastVersion(0L)
        .withDeletedAt(0L)
        .build();
  }
}
