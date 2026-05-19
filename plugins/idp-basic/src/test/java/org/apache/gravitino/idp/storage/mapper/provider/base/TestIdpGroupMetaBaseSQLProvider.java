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
import java.util.Map;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpGroupMetaBaseSQLProvider {

  protected IdpGroupMetaBaseSQLProvider createProvider() {
    return new IdpGroupMetaBaseSQLProvider() {
      @Override
      protected String currentTimeMillisExpression() {
        return "CURRENT_TIME_MILLIS()";
      }
    };
  }

  protected String expectedDeleteAtClause() {
    return "deleted_at = CURRENT_TIME_MILLIS()";
  }

  protected String expectedDeleteIdpGroupMetasByLegacyTimelineSql() {
    return "DELETE FROM idp_group_meta WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline}"
        + " LIMIT #{limit}";
  }

  @Test
  void testSelectIdpGroup() {
    String normalizedSql = createProvider().selectIdpGroup("group").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("SELECT group_id as groupId"));
    Assertions.assertTrue(normalizedSql.contains("FROM idp_group_meta"));
    Assertions.assertTrue(
        normalizedSql.contains("WHERE group_name = #{groupName} AND deleted_at = 0"));
  }

  @Test
  void testSelectIdpGroups() {
    String script = createProvider().selectIdpGroups(Arrays.asList("dev", "ops"));
    Map<String, Object> params = new HashMap<>();
    params.put("groupNames", Arrays.asList("dev", "ops"));

    String normalizedSql = renderScript(script, params);

    Assertions.assertTrue(normalizedSql.contains("SELECT group_id as groupId"));
    Assertions.assertTrue(normalizedSql.contains("FROM idp_group_meta"));
    Assertions.assertTrue(
        normalizedSql.matches(".*group_name IN\\s*\\(\\s*\\?\\s*,\\s*\\?\\s*\\).*"));
    Assertions.assertFalse(normalizedSql.matches(".*\\b1\\s*=\\s*0\\b.*"));
  }

  @Test
  void testSelectIdpGroupsWithEmptyGroupNames() {
    String script = createProvider().selectIdpGroups(Collections.emptyList());
    Map<String, Object> params = new HashMap<>();
    params.put("groupNames", Collections.emptyList());

    String normalizedSql = renderScript(script, params);

    Assertions.assertFalse(
        normalizedSql.matches(".*\\bIN\\s*\\(\\s*\\).*"),
        "Empty groupNames should not generate invalid SQL IN (...) with no values");
    Assertions.assertFalse(normalizedSql.matches(".*\\b1\\s*=\\s*0\\b.*"));
    Assertions.assertEquals(
        "SELECT group_id as groupId, group_name as groupName, current_version as"
            + " currentVersion, last_version as lastVersion, deleted_at as deletedAt FROM"
            + " idp_group_meta WHERE deleted_at = 0",
        normalizedSql);
  }

  @Test
  void testSelectIdpGroupsWithNullGroupNames() {
    String script = createProvider().selectIdpGroups(null);
    Map<String, Object> params = new HashMap<>();
    params.put("groupNames", null);

    Assertions.assertThrows(BuilderException.class, () -> renderScript(script, params));
  }

  @Test
  void testInsertIdpGroup() {
    String normalizedSql =
        createProvider().insertIdpGroup(newGroupPO()).replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("INSERT INTO idp_group_meta"));
    Assertions.assertTrue(
        normalizedSql.contains(
            "(group_id, group_name, current_version, last_version, deleted_at)"));
    Assertions.assertTrue(
        normalizedSql.contains(
            "VALUES ( #{groupMeta.groupId}, #{groupMeta.groupName}, #{groupMeta.currentVersion},"
                + " #{groupMeta.lastVersion}, #{groupMeta.deletedAt} )"));
  }

  @Test
  void testSoftDeleteIdpGroup() {
    String normalizedSql = createProvider().softDeleteIdpGroup(1L).replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_group_meta"));
    Assertions.assertTrue(normalizedSql.contains(expectedDeleteAtClause()));
    Assertions.assertTrue(normalizedSql.contains("WHERE group_id = #{groupId} AND deleted_at = 0"));
  }

  @Test
  void testDeleteIdpGroupMetasByLegacyTimeline() {
    String normalizedSql =
        createProvider().deleteIdpGroupMetasByLegacyTimeline(1L, 2).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(expectedDeleteIdpGroupMetasByLegacyTimelineSql(), normalizedSql);
  }

  @Test
  void testCurrentTimeMillisExpression() {
    Assertions.assertEquals(
        "(UNIX_TIMESTAMP() * 1000.0)",
        new IdpGroupMetaBaseSQLProvider().currentTimeMillisExpression());
  }

  private IdpGroupPO newGroupPO() {
    return IdpGroupPO.builder()
        .withGroupId(1L)
        .withGroupName("group")
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private String renderScript(String script, Map<String, Object> params) {
    SqlSource sqlSource =
        new XMLLanguageDriver().createSqlSource(new Configuration(), script, Map.class);
    BoundSql boundSql = sqlSource.getBoundSql(params);
    return boundSql.getSql().replaceAll("\\s+", " ").trim();
  }
}
