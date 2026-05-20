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
    Assertions.assertEquals(
        "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)",
        new IdpUserGroupRelPostgreSQLProvider().currentTimeMillisExpression());
  }

  @Test
  void testSoftDeleteRelations() {
    String normalizedSql =
        new IdpUserGroupRelPostgreSQLProvider()
            .softDeleteRelations("dev", Arrays.asList("alice", "bob"))
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(
        "<script>UPDATE idp_user_group_rel r SET deleted_at = CAST(EXTRACT(EPOCH FROM"
            + " CURRENT_TIMESTAMP) * 1000 AS BIGINT) FROM idp_group_meta g, idp_user_meta u WHERE"
            + " r.group_id = g.group_id AND r.user_id = u.user_id AND g.group_name = #{groupName}"
            + " AND g.deleted_at = 0 AND u.deleted_at = 0 AND r.deleted_at = 0<foreach"
            + " collection='usernames' item='username' open=' AND u.user_name IN (' separator=','"
            + " close=')'>#{username}</foreach></script>",
        normalizedSql);
  }

  @Test
  void testSoftDeleteRelationsByUsername() {
    String normalizedSql =
        new IdpUserGroupRelPostgreSQLProvider()
            .softDeleteRelationsByUsername("alice")
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(
        "UPDATE idp_user_group_rel r SET deleted_at = CAST(EXTRACT(EPOCH FROM"
            + " CURRENT_TIMESTAMP) * 1000 AS BIGINT) FROM idp_user_meta u WHERE r.user_id ="
            + " u.user_id AND u.user_name = #{username} AND u.deleted_at = 0 AND r.deleted_at = 0",
        normalizedSql);
  }

  @Test
  void testSoftDeleteRelationsByGroupName() {
    String normalizedSql =
        new IdpUserGroupRelPostgreSQLProvider()
            .softDeleteRelationsByGroupName("dev")
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(
        "UPDATE idp_user_group_rel r SET deleted_at = CAST(EXTRACT(EPOCH FROM"
            + " CURRENT_TIMESTAMP) * 1000 AS BIGINT) FROM idp_group_meta g WHERE r.group_id ="
            + " g.group_id AND g.group_name = #{groupName} AND g.deleted_at = 0 AND r.deleted_at ="
            + " 0",
        normalizedSql);
  }

  @Test
  void testDeleteIdpUserGroupRelMetasByLegacyTimeline() {
    Assertions.assertEquals(
        "DELETE FROM idp_user_group_rel WHERE id IN (SELECT id FROM idp_user_group_rel"
            + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})",
        new IdpUserGroupRelPostgreSQLProvider().deleteIdpUserGroupRelMetasByLegacyTimeline(1L, 2));
  }
}
