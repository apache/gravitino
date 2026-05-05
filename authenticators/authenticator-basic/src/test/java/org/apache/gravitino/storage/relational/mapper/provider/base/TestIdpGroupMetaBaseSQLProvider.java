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

import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpGroupMetaBaseSQLProvider {

  protected IdpGroupMetaBaseSQLProvider createProvider() {
    return new IdpGroupMetaBaseSQLProvider();
  }

  protected String expectedDeleteAtClause() {
    return "deleted_at = #{deletedAt}";
  }

  @Test
  public void testSelectLocalGroup() {
    String normalizedSql =
        createProvider().selectLocalGroup("group").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("SELECT group_id as groupId"));
    Assertions.assertTrue(normalizedSql.contains("FROM idp_group_meta"));
    Assertions.assertTrue(
        normalizedSql.contains("WHERE group_name = #{groupName} AND deleted_at = 0"));
  }

  @Test
  public void testInsertLocalGroup() {
    String normalizedSql =
        createProvider().insertLocalGroup(newGroupPO()).replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("INSERT INTO idp_group_meta"));
    Assertions.assertTrue(
        normalizedSql.contains(
            "(group_id, group_name, audit_info, current_version, last_version, deleted_at)"));
    Assertions.assertTrue(
        normalizedSql.contains(
            "VALUES ( #{groupMeta.groupId}, #{groupMeta.groupName}, #{groupMeta.auditInfo}, #{groupMeta.currentVersion}, #{groupMeta.lastVersion}, #{groupMeta.deletedAt} )"));
  }

  @Test
  public void testSoftDeleteLocalGroup() {
    String normalizedSql =
        createProvider().softDeleteLocalGroup(1L, 2L, "audit").replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(normalizedSql.contains("UPDATE idp_group_meta"));
    Assertions.assertTrue(normalizedSql.contains(expectedDeleteAtClause()));
    Assertions.assertTrue(normalizedSql.contains("audit_info = #{auditInfo}"));
    Assertions.assertTrue(normalizedSql.contains("current_version = current_version + 1"));
    Assertions.assertTrue(normalizedSql.contains("last_version = last_version + 1"));
    Assertions.assertTrue(normalizedSql.contains("WHERE group_id = #{groupId} AND deleted_at = 0"));
  }

  @Test
  public void testSoftDeleteLocalGroupDoesNotUseOptimisticLocking() {
    IdpGroupMetaBaseSQLProvider provider = createProvider();

    String normalizedSql =
        provider.softDeleteLocalGroup(1L, 2L, "audit").replaceAll("\\s+", " ").trim();

    Assertions.assertFalse(
        normalizedSql.contains("current_version = #{currentVersion}"),
        "Built-in IdP group delete should not use optimistic locking");
    Assertions.assertTrue(
        normalizedSql.contains("current_version = current_version + 1"),
        "Built-in IdP group delete should increment current_version in place");
    Assertions.assertTrue(
        normalizedSql.contains("last_version = last_version + 1"),
        "Built-in IdP group delete should increment last_version in place");
  }

  @Test
  public void testTruncateLocalGroupMeta() {
    Assertions.assertEquals(
        "DELETE FROM idp_group_meta", createProvider().truncateLocalGroupMeta());
  }

  private IdpGroupPO newGroupPO() {
    return IdpGroupPO.builder()
        .withGroupId(1L)
        .withGroupName("group")
        .withAuditInfo("audit")
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }
}
