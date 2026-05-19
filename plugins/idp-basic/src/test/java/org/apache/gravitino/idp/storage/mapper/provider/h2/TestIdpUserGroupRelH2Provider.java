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
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.idp.storage.mapper.AbstractIdpMetaStorageTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIdpUserGroupRelH2Provider extends AbstractIdpMetaStorageTest {

  @Test
  void testSoftDeleteRelations() {
    IdpUserGroupRelH2Provider provider = new IdpUserGroupRelH2Provider();
    String script = provider.softDeleteRelations("dev", Arrays.asList("alice", "bob"));
    Map<String, Object> params = new HashMap<>();
    params.put("groupName", "dev");
    params.put("usernames", Arrays.asList("alice", "bob"));

    String normalizedSql = renderScript(script, params);

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_user_group_rel r SET deleted_at ="));
    Assertions.assertTrue(
        normalizedSql.contains(
            "r.group_id IN (SELECT g.group_id FROM idp_group_meta g WHERE g.group_name = ?"));
    Assertions.assertTrue(
        normalizedSql.contains("r.user_id IN (SELECT u.user_id FROM idp_user_meta u"));
    Assertions.assertTrue(normalizedSql.matches(".*u.user_name IN \\( \\? , \\? \\).*"));
  }

  @Test
  void testSoftDeleteRelationsByUsername() {
    IdpUserGroupRelH2Provider provider = new IdpUserGroupRelH2Provider();
    String normalizedSql =
        provider.softDeleteRelationsByUsername("alice").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.startsWith("MERGE INTO idp_user_group_rel r USING"));
    Assertions.assertTrue(normalizedSql.contains("idp_user_meta u ON r.user_id = u.user_id"));
    Assertions.assertTrue(normalizedSql.contains("u.user_name = #{username}"));
    Assertions.assertTrue(normalizedSql.contains("WHEN MATCHED THEN UPDATE SET r.deleted_at ="));
  }

  @Test
  void testSoftDeleteRelationsByGroupName() {
    IdpUserGroupRelH2Provider provider = new IdpUserGroupRelH2Provider();
    String normalizedSql =
        provider.softDeleteRelationsByGroupName("dev").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.startsWith("MERGE INTO idp_user_group_rel r USING"));
    Assertions.assertTrue(normalizedSql.contains("idp_group_meta g ON r.group_id = g.group_id"));
    Assertions.assertTrue(normalizedSql.contains("g.group_name = #{groupName}"));
    Assertions.assertTrue(normalizedSql.contains("WHEN MATCHED THEN UPDATE SET r.deleted_at ="));
  }

  @Test
  void testCurrentTimeMillisExpression() {
    IdpUserGroupRelH2Provider provider = new IdpUserGroupRelH2Provider();

    Assertions.assertEquals(
        "DATEDIFF('MILLISECOND', TIMESTAMP '1970-01-01 00:00:00', CURRENT_TIMESTAMP())",
        provider.currentTimeMillisExpression());
  }
}
