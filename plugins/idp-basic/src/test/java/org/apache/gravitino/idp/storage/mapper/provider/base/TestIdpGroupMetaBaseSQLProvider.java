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
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpGroupMetaBaseSQLProvider {

  @Test
  void testSelectIdpGroup() {
    String normalizedSql = createProvider().selectIdpGroup("group").replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(
        "SELECT group_id as groupId, group_name as groupName, current_version as"
            + " currentVersion, last_version as lastVersion, deleted_at as deletedAt FROM"
            + " idp_group_meta WHERE group_name = #{groupName} AND deleted_at = 0",
        normalizedSql);
  }

  @Test
  void testSelectIdpGroups() {
    String normalizedSql =
        createProvider()
            .selectIdpGroups(Arrays.asList("dev", "ops"))
            .replaceAll("\\s+", " ")
            .trim();

    Assertions.assertEquals(
        "<script>SELECT group_id as groupId, group_name as groupName, current_version as"
            + " currentVersion, last_version as lastVersion, deleted_at as deletedAt FROM"
            + " idp_group_meta WHERE deleted_at = 0 <foreach collection='groupNames'"
            + " item='groupName' open='AND group_name IN (' separator=',' close=')'>#{groupName}"
            + "</foreach></script>",
        normalizedSql);
  }

  @Test
  void testSelectIdpGroupsWithEmptyGroupNames() {
    String normalizedSql =
        createProvider().selectIdpGroups(Collections.emptyList()).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(
        "<script>SELECT group_id as groupId, group_name as groupName, current_version as"
            + " currentVersion, last_version as lastVersion, deleted_at as deletedAt FROM"
            + " idp_group_meta WHERE deleted_at = 0 <foreach collection='groupNames'"
            + " item='groupName' open='AND group_name IN (' separator=',' close=')'>#{groupName}"
            + "</foreach></script>",
        normalizedSql);
  }

  @Test
  void testInsertIdpGroup() {
    String normalizedSql =
        createProvider().insertIdpGroup(newGroupPO()).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(
        "INSERT INTO idp_group_meta (group_id, group_name, current_version, last_version,"
            + " deleted_at) VALUES ( #{groupMeta.groupId}, #{groupMeta.groupName},"
            + " #{groupMeta.currentVersion}, #{groupMeta.lastVersion}, #{groupMeta.deletedAt} )",
        normalizedSql);
  }

  @Test
  void testSoftDeleteIdpGroup() {
    String normalizedSql = createProvider().softDeleteIdpGroup(1L).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(
        "UPDATE idp_group_meta SET deleted_at = CURRENT_TIME_MILLIS() WHERE group_id ="
            + " #{groupId} AND deleted_at = 0",
        normalizedSql);
  }

  @Test
  void testDeleteIdpGroupMetasByLegacyTimeline() {
    String normalizedSql =
        createProvider().deleteIdpGroupMetasByLegacyTimeline(1L, 2).replaceAll("\\s+", " ").trim();

    Assertions.assertEquals(
        "DELETE FROM idp_group_meta WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline}"
            + " LIMIT #{limit}",
        normalizedSql);
  }

  @Test
  void testCurrentTimeMillisExpression() {
    Assertions.assertEquals(
        "(UNIX_TIMESTAMP() * 1000.0)",
        new IdpGroupMetaBaseSQLProvider().currentTimeMillisExpression());
  }

  private IdpGroupMetaBaseSQLProvider createProvider() {
    return new IdpGroupMetaBaseSQLProvider() {
      @Override
      protected String currentTimeMillisExpression() {
        return "CURRENT_TIME_MILLIS()";
      }
    };
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
}
