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
import java.util.List;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserGroupRelBaseSQLProvider {

  @Test
  void testSelectGroupNamesByUsername() {
    String normalizedSql =
        createProvider().selectGroupNamesByUsername("alice").replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(
        "SELECT g.group_name FROM idp_user_meta u JOIN idp_user_group_rel r ON r.user_id ="
            + " u.user_id AND r.deleted_at = 0 JOIN idp_group_meta g ON g.group_id = r.group_id"
            + " AND g.deleted_at = 0 WHERE u.user_name = #{username} AND u.deleted_at = 0 ORDER BY"
            + " g.group_name",
        normalizedSql);
  }

  @Test
  void testSelectUsernamesByGroupName() {
    String normalizedSql =
        createProvider().selectUsernamesByGroupName("dev").replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(
        "SELECT u.user_name FROM idp_group_meta g JOIN idp_user_group_rel r ON r.group_id ="
            + " g.group_id AND r.deleted_at = 0 JOIN idp_user_meta u ON u.user_id = r.user_id AND"
            + " u.deleted_at = 0 WHERE g.group_name = #{groupName} AND g.deleted_at = 0 ORDER BY"
            + " u.user_name",
        normalizedSql);
  }

  @Test
  void testBatchInsertIdpUserGroups() {
    List<IdpUserGroupRelPO> relations =
        Arrays.asList(newRelation(1L, 20L, 10L), newRelation(2L, 21L, 10L));

    String normalizedSql =
        createProvider().batchInsertRelations(relations).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(
        "<script>INSERT INTO idp_user_group_rel (id, user_id, group_id, current_version,"
            + " last_version, deleted_at) VALUES <foreach item='item' collection='relations'"
            + " separator=','>(#{item.id}, #{item.userId}, #{item.groupId}, #{item.currentVersion},"
            + " #{item.lastVersion}, #{item.deletedAt})</foreach></script>",
        normalizedSql);
  }

  @Test
  void testSoftDeleteIdpUserGroups() {
    String normalizedSql =
        createProvider()
            .softDeleteRelations("dev", Arrays.asList("alice", "bob"))
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(
        "<script>UPDATE idp_user_group_rel r INNER JOIN idp_group_meta g ON g.group_id ="
            + " r.group_id AND g.deleted_at = 0 INNER JOIN idp_user_meta u ON u.user_id = r.user_id"
            + " AND u.deleted_at = 0 SET r.deleted_at = CURRENT_TIME_MILLIS() WHERE g.group_name ="
            + " #{groupName} AND r.deleted_at = 0<foreach collection='usernames' item='username'"
            + " open=' AND u.user_name IN (' separator=',' close=')'>#{username}</foreach></script>",
        normalizedSql);
  }

  @Test
  void testSoftDeleteRelationsByUsername() {
    String normalizedSql =
        createProvider().softDeleteRelationsByUsername("alice").replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(
        "UPDATE idp_user_group_rel r INNER JOIN idp_user_meta u ON u.user_id = r.user_id AND"
            + " u.deleted_at = 0 SET r.deleted_at = CURRENT_TIME_MILLIS() WHERE u.user_name ="
            + " #{username} AND r.deleted_at = 0",
        normalizedSql);
  }

  @Test
  void testSoftDeleteRelationsByGroupName() {
    String normalizedSql =
        createProvider().softDeleteRelationsByGroupName("dev").replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(
        "UPDATE idp_user_group_rel r INNER JOIN idp_group_meta g ON g.group_id = r.group_id"
            + " AND g.deleted_at = 0 SET r.deleted_at = CURRENT_TIME_MILLIS() WHERE g.group_name ="
            + " #{groupName} AND r.deleted_at = 0",
        normalizedSql);
  }

  @Test
  void testDeleteIdpUserGroupRelMetasByLegacyTimeline() {
    String normalizedSql =
        createProvider()
            .deleteIdpUserGroupRelMetasByLegacyTimeline(1L, 2)
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(
        "DELETE FROM idp_user_group_rel WHERE deleted_at > 0 AND deleted_at <"
            + " #{legacyTimeline} LIMIT #{limit}",
        normalizedSql);
  }

  @Test
  void testCurrentTimeMillisExpression() {
    Assertions.assertEquals(
        "(UNIX_TIMESTAMP() * 1000.0)",
        new IdpUserGroupRelBaseSQLProvider().currentTimeMillisExpression());
  }

  private IdpUserGroupRelBaseSQLProvider createProvider() {
    return new IdpUserGroupRelBaseSQLProvider() {
      @Override
      protected String currentTimeMillisExpression() {
        return "CURRENT_TIME_MILLIS()";
      }
    };
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
