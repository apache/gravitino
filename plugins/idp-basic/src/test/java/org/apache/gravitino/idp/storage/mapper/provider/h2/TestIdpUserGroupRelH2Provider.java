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

package org.apache.gravitino.idp.storage.mapper.provider.h2;

import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserGroupRelH2Provider {

  @Test
  void testSoftDeleteRelations() {
    String normalizedSql =
        new IdpUserGroupRelH2Provider()
            .softDeleteRelations("dev", Arrays.asList("alice", "bob"))
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(
        "<script>UPDATE idp_user_group_rel r SET deleted_at = DATEDIFF('MILLISECOND',"
            + " TIMESTAMP '1970-01-01 00:00:00', CURRENT_TIMESTAMP()) WHERE r.deleted_at = 0 AND"
            + " r.group_id IN (SELECT g.group_id FROM idp_group_meta g WHERE g.group_name ="
            + " #{groupName} AND g.deleted_at = 0) AND r.user_id IN (SELECT u.user_id FROM"
            + " idp_user_meta u WHERE u.deleted_at = 0<foreach collection='usernames'"
            + " item='username' open=' AND u.user_name IN (' separator=',' close=')'>#{username}"
            + "</foreach>)</script>",
        normalizedSql);
  }

  @Test
  void testSoftDeleteRelationsByUsername() {
    String normalizedSql =
        new IdpUserGroupRelH2Provider()
            .softDeleteRelationsByUsername("alice")
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(
        "MERGE INTO idp_user_group_rel r USING idp_user_meta u ON r.user_id = u.user_id"
            + " AND u.user_name = #{username} AND u.deleted_at = 0 AND r.deleted_at = 0 WHEN"
            + " MATCHED THEN UPDATE SET r.deleted_at = DATEDIFF('MILLISECOND', TIMESTAMP"
            + " '1970-01-01 00:00:00', CURRENT_TIMESTAMP())",
        normalizedSql);
  }

  @Test
  void testSoftDeleteRelationsByGroupName() {
    String normalizedSql =
        new IdpUserGroupRelH2Provider()
            .softDeleteRelationsByGroupName("dev")
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(
        "MERGE INTO idp_user_group_rel r USING idp_group_meta g ON r.group_id = g.group_id"
            + " AND g.group_name = #{groupName} AND g.deleted_at = 0 AND r.deleted_at = 0 WHEN"
            + " MATCHED THEN UPDATE SET r.deleted_at = DATEDIFF('MILLISECOND', TIMESTAMP"
            + " '1970-01-01 00:00:00', CURRENT_TIMESTAMP())",
        normalizedSql);
  }

  @Test
  void testCurrentTimeMillisExpression() {
    Assertions.assertEquals(
        "DATEDIFF('MILLISECOND', TIMESTAMP '1970-01-01 00:00:00', CURRENT_TIMESTAMP())",
        new IdpUserGroupRelH2Provider().currentTimeMillisExpression());
  }
}
