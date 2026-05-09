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
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserMetaBaseSQLProvider {

  protected IdpUserMetaBaseSQLProvider createProvider() {
    return new IdpUserMetaBaseSQLProvider();
  }

  protected String expectedDeleteAtClause() {
    return "deleted_at = #{deletedAt}";
  }

  protected String expectedDeleteIdpUserMetasByLegacyTimelineSql() {
    return "DELETE FROM idp_user_meta WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  @Test
  public void testSelectIdpUser() {
    String normalizedSql = createProvider().selectIdpUser("tom").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("SELECT user_id as userId"));
    Assertions.assertTrue(normalizedSql.contains("FROM idp_user_meta"));
    Assertions.assertTrue(
        normalizedSql.contains("WHERE user_name = #{userName} AND deleted_at = 0"));
  }

  @Test
  public void testSelectIdpUsers() {
    String script = createProvider().selectIdpUsers(Arrays.asList("tom", "jerry"));
    Map<String, Object> params = new HashMap<>();
    params.put("userNames", Arrays.asList("tom", "jerry"));

    String normalizedSql = renderScript(script, params);

    Assertions.assertTrue(normalizedSql.contains("SELECT user_id as userId"));
    Assertions.assertTrue(normalizedSql.contains("FROM idp_user_meta"));
    Assertions.assertTrue(normalizedSql.matches(".*user_name IN \\( \\? , \\? \\).*"));
  }

  @Test
  public void testInsertIdpUser() {
    String normalizedSql =
        createProvider().insertIdpUser(newUserPO()).replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("INSERT INTO idp_user_meta"));
    Assertions.assertTrue(
        normalizedSql.contains(
            "(user_id, user_name, password_hash, audit_info, current_version, last_version, deleted_at)"));
    Assertions.assertTrue(
        normalizedSql.contains(
            "VALUES ( #{userMeta.userId}, #{userMeta.userName}, #{userMeta.passwordHash}, #{userMeta.auditInfo}, #{userMeta.currentVersion}, #{userMeta.lastVersion}, #{userMeta.deletedAt} )"));
  }

  @Test
  public void testUpdateIdpUserPassword() {
    String normalizedSql =
        createProvider()
            .updateIdpUserPassword(1L, "hash", "audit", 1L, 2L, 3L)
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_user_meta"));
    Assertions.assertTrue(normalizedSql.contains("SET password_hash = #{passwordHash}"));
    Assertions.assertTrue(normalizedSql.contains("current_version = #{newCurrentVersion}"));
    Assertions.assertTrue(normalizedSql.contains("last_version = #{newLastVersion}"));
    Assertions.assertTrue(normalizedSql.contains("WHERE user_id = #{userId}"));
    Assertions.assertTrue(normalizedSql.contains("AND current_version = #{currentVersion}"));
    Assertions.assertTrue(normalizedSql.contains("AND deleted_at = 0"));
  }

  @Test
  public void testSoftDeleteIdpUser() {
    String normalizedSql =
        createProvider().softDeleteIdpUser(1L, 2L, "audit").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_user_meta"));
    Assertions.assertTrue(normalizedSql.contains(expectedDeleteAtClause()));
    Assertions.assertTrue(normalizedSql.contains("audit_info = #{auditInfo}"));
    Assertions.assertTrue(normalizedSql.contains("current_version = current_version + 1"));
    Assertions.assertTrue(normalizedSql.contains("last_version = last_version + 1"));
    Assertions.assertTrue(normalizedSql.contains("WHERE user_id = #{userId} AND deleted_at = 0"));
  }

  @Test
  public void testSoftDeleteIdpUserDoesNotUseOptimisticLocking() {
    IdpUserMetaBaseSQLProvider provider = createProvider();

    String normalizedSql =
        provider.softDeleteIdpUser(1L, 2L, "audit").replaceAll("\\s+", " ").trim();

    Assertions.assertFalse(
        normalizedSql.contains("current_version = #{currentVersion}"),
        "Built-in IdP user delete should not use optimistic locking");
    Assertions.assertTrue(
        normalizedSql.contains("current_version = current_version + 1"),
        "Built-in IdP user delete should increment current_version in place");
    Assertions.assertTrue(
        normalizedSql.contains("last_version = last_version + 1"),
        "Built-in IdP user delete should increment last_version in place");
  }

  @Test
  public void testDeleteIdpUserMetasByLegacyTimeline() {
    String normalizedSql =
        createProvider().deleteIdpUserMetasByLegacyTimeline(1L, 2).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(expectedDeleteIdpUserMetasByLegacyTimelineSql(), normalizedSql);
  }

  private String renderScript(String script, Map<String, Object> params) {
    SqlSource sqlSource =
        new XMLLanguageDriver().createSqlSource(new Configuration(), script, Map.class);
    BoundSql boundSql = sqlSource.getBoundSql(params);
    return boundSql.getSql().replaceAll("\\s+", " ").trim();
  }

  private IdpUserPO newUserPO() {
    return IdpUserPO.builder()
        .withUserId(1L)
        .withUserName("tom")
        .withPasswordHash("hash")
        .withAuditInfo("audit")
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }
}
