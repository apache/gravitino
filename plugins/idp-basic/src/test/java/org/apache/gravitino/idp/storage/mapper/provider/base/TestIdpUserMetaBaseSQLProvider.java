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
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserMetaBaseSQLProvider {

  @Test
  void testSelectIdpUser() {
    String normalizedSql = createProvider().selectIdpUser("tom").replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(expectedSelectIdpUserSql(), normalizedSql);
  }

  @Test
  void testSelectIdpUsers() {
    String normalizedSql =
        createProvider()
            .selectIdpUsers(Arrays.asList("tom", "jerry"))
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(expectedSelectIdpUsersSql(), normalizedSql);
  }

  @Test
  void testSelectIdpUsersWithEmptyUserNames() {
    String normalizedSql =
        createProvider().selectIdpUsers(Collections.emptyList()).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(expectedSelectIdpUsersSql(), normalizedSql);
  }

  @Test
  void testInsertIdpUser() {
    String normalizedSql =
        createProvider().insertIdpUser(newUserPO()).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(expectedInsertIdpUserSql(), normalizedSql);
  }

  @Test
  void testUpdateIdpUserPassword() {
    String normalizedSql =
        createProvider().updateIdpUserPassword(1L, "hash").replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(expectedUpdateIdpUserPasswordSql(), normalizedSql);
  }

  @Test
  void testSoftDeleteIdpUser() {
    String normalizedSql = createProvider().softDeleteIdpUser(1L).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(expectedSoftDeleteIdpUserSql(), normalizedSql);
  }

  @Test
  void testDeleteIdpUserMetasByLegacyTimeline() {
    String normalizedSql =
        createProvider().deleteIdpUserMetasByLegacyTimeline(1L, 2).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(expectedDeleteIdpUserMetasByLegacyTimelineSql(), normalizedSql);
  }

  @Test
  void testCurrentTimeMillisExpression() {
    Assertions.assertEquals(
        "(UNIX_TIMESTAMP() * 1000.0)",
        new IdpUserMetaBaseSQLProvider().currentTimeMillisExpression());
  }

  private String expectedSelectIdpUserSql() {
    return "SELECT user_id as userId, user_name as userName, password_hash as passwordHash,"
        + " current_version as currentVersion, last_version as lastVersion, deleted_at as"
        + " deletedAt FROM idp_user_meta WHERE user_name = #{username} AND deleted_at = 0";
  }

  private String expectedSelectIdpUsersSql() {
    return "<script>SELECT user_id as userId, user_name as userName, password_hash as"
        + " passwordHash, current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt FROM idp_user_meta WHERE deleted_at = 0 <foreach"
        + " collection='usernames' item='username' open='AND user_name IN (' separator=','"
        + " close=')'>#{username}</foreach></script>";
  }

  private String expectedInsertIdpUserSql() {
    return "INSERT INTO idp_user_meta (user_id, user_name, password_hash, current_version,"
        + " last_version, deleted_at) VALUES ( #{userMeta.userId}, #{userMeta.userName},"
        + " #{userMeta.passwordHash}, #{userMeta.currentVersion}, #{userMeta.lastVersion},"
        + " #{userMeta.deletedAt} )";
  }

  private String expectedUpdateIdpUserPasswordSql() {
    return "UPDATE idp_user_meta SET password_hash = #{passwordHash} WHERE user_id = #{userId}"
        + " AND deleted_at = 0";
  }

  private String expectedSoftDeleteIdpUserSql() {
    return "UPDATE idp_user_meta SET deleted_at = CURRENT_TIME_MILLIS() WHERE user_id ="
        + " #{userId} AND deleted_at = 0";
  }

  private String expectedDeleteIdpUserMetasByLegacyTimelineSql() {
    return "DELETE FROM idp_user_meta WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline}"
        + " LIMIT #{limit}";
  }

  private IdpUserMetaBaseSQLProvider createProvider() {
    return new IdpUserMetaBaseSQLProvider() {
      @Override
      protected String currentTimeMillisExpression() {
        return "CURRENT_TIME_MILLIS()";
      }
    };
  }

  private IdpUserPO newUserPO() {
    return IdpUserPO.builder()
        .withUserId(1L)
        .withUserName("tom")
        .withPasswordHash("hash")
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }
}
