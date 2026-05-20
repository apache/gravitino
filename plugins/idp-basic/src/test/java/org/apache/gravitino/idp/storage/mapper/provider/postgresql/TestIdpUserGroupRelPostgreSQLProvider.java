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

package org.apache.gravitino.idp.storage.mapper.provider.postgresql;

import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserGroupRelPostgreSQLProvider {

  @Test
  void testCurrentTimeMillisExpression() {
    IdpUserGroupRelPostgreSQLProvider provider = new IdpUserGroupRelPostgreSQLProvider();

    Assertions.assertEquals(
        "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)",
        provider.currentTimeMillisExpression());
  }

  @Test
  void testSoftDeleteRelations() {
    IdpUserGroupRelPostgreSQLProvider provider = new IdpUserGroupRelPostgreSQLProvider();
    String normalizedSql =
        provider
            .softDeleteRelations("dev", Arrays.asList("alice", "bob"))
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(expectedSoftDeleteRelationsSql(), normalizedSql);
  }

  @Test
  void testSoftDeleteRelationsByUsername() {
    IdpUserGroupRelPostgreSQLProvider provider = new IdpUserGroupRelPostgreSQLProvider();
    String normalizedSql =
        provider.softDeleteRelationsByUsername("alice").replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(expectedSoftDeleteRelationsByUsernameSql(), normalizedSql);
  }

  @Test
  void testSoftDeleteRelationsByGroupName() {
    IdpUserGroupRelPostgreSQLProvider provider = new IdpUserGroupRelPostgreSQLProvider();
    String normalizedSql =
        provider.softDeleteRelationsByGroupName("dev").replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(expectedSoftDeleteRelationsByGroupNameSql(), normalizedSql);
  }

  @Test
  void testDeleteIdpUserGroupRelMetasByLegacyTimeline() {
    IdpUserGroupRelPostgreSQLProvider provider = new IdpUserGroupRelPostgreSQLProvider();

    Assertions.assertEquals(
        expectedDeleteIdpUserGroupRelMetasByLegacyTimelineSql(),
        provider.deleteIdpUserGroupRelMetasByLegacyTimeline(1L, 2));
  }

  private String expectedSoftDeleteRelationsSql() {
    return "<script>UPDATE idp_user_group_rel r SET deleted_at = CAST(EXTRACT(EPOCH FROM"
        + " CURRENT_TIMESTAMP) * 1000 AS BIGINT) FROM idp_group_meta g, idp_user_meta u WHERE"
        + " r.group_id = g.group_id AND r.user_id = u.user_id AND g.group_name = #{groupName}"
        + " AND g.deleted_at = 0 AND u.deleted_at = 0 AND r.deleted_at = 0<foreach"
        + " collection='usernames' item='username' open=' AND u.user_name IN (' separator=','"
        + " close=')'>#{username}</foreach></script>";
  }

  private String expectedSoftDeleteRelationsByUsernameSql() {
    return "UPDATE idp_user_group_rel r SET deleted_at = CAST(EXTRACT(EPOCH FROM"
        + " CURRENT_TIMESTAMP) * 1000 AS BIGINT) FROM idp_user_meta u WHERE r.user_id ="
        + " u.user_id AND u.user_name = #{username} AND u.deleted_at = 0 AND r.deleted_at = 0";
  }

  private String expectedSoftDeleteRelationsByGroupNameSql() {
    return "UPDATE idp_user_group_rel r SET deleted_at = CAST(EXTRACT(EPOCH FROM"
        + " CURRENT_TIMESTAMP) * 1000 AS BIGINT) FROM idp_group_meta g WHERE r.group_id ="
        + " g.group_id AND g.group_name = #{groupName} AND g.deleted_at = 0 AND r.deleted_at ="
        + " 0";
  }

  private String expectedDeleteIdpUserGroupRelMetasByLegacyTimelineSql() {
    return "DELETE FROM idp_user_group_rel WHERE id IN (SELECT id FROM idp_user_group_rel"
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
