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

package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.TestTemplate;

class TestIdpGroupMetaService extends TestJDBCBackend {

  private static final String AUDIT_INFO_JSON = "{\"creator\":\"creator\"}";

  @TestTemplate
  void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    IdpUserMetaService userMetaService = IdpUserMetaService.getInstance();
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();

    IdpUserPO user =
        createIdpUserPO(RandomIdGenerator.INSTANCE.nextId(), "idp-user", "hashed-password");
    userMetaService.createUser(user);

    IdpGroupPO group = createIdpGroupPO(RandomIdGenerator.INSTANCE.nextId(), "idp-group");
    IdpGroupPO anotherGroup =
        createIdpGroupPO(RandomIdGenerator.INSTANCE.nextId(), "another-idp-group");
    groupMetaService.createGroup(group);
    groupMetaService.createGroup(anotherGroup);
    groupMetaService.addUsersToGroup(
        Collections.singletonList(
            createIdpGroupUserRelPO(RandomIdGenerator.INSTANCE.nextId(), group, user)));

    assertTrue(groupMetaService.findGroup(group.getGroupName()).isPresent());
    assertTrue(groupMetaService.findGroup(anotherGroup.getGroupName()).isPresent());
    assertEquals(2, countIdpGroups());
    assertEquals(1, countIdpGroupUserRels());

    assertEquals(0, backend.hardDeleteLegacyData(Entity.EntityType.IDP_GROUP, futureTimestamp()));
    assertEquals(2, countIdpGroups());
    assertEquals(1, countIdpGroupUserRels());

    long deletedAt = Instant.now().toEpochMilli();
    assertTrue(groupMetaService.deleteGroup(group, deletedAt, AUDIT_INFO_JSON));

    assertFalse(groupMetaService.findGroup(group.getGroupName()).isPresent());
    assertTrue(groupMetaService.findGroup(anotherGroup.getGroupName()).isPresent());
    assertTrue(legacyRecordExistsInDB(group.getGroupId(), Entity.EntityType.IDP_GROUP));
    assertEquals(2, countIdpGroups());
    assertEquals(1, countIdpGroupUserRels());

    assertEquals(2, backend.hardDeleteLegacyData(Entity.EntityType.IDP_GROUP, futureTimestamp()));
    assertFalse(legacyRecordExistsInDB(group.getGroupId(), Entity.EntityType.IDP_GROUP));
    assertTrue(groupMetaService.findGroup(anotherGroup.getGroupName()).isPresent());
    assertEquals(1, countIdpGroups());
    assertEquals(0, countIdpGroupUserRels());
  }

  @TestTemplate
  void testListGroupNamesByUserId() throws IOException {
    IdpUserMetaService userMetaService = IdpUserMetaService.getInstance();
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();

    IdpUserPO user =
        createIdpUserPO(RandomIdGenerator.INSTANCE.nextId(), "idp-user", "hashed-password");
    userMetaService.createUser(user);

    IdpGroupPO groupB = createIdpGroupPO(RandomIdGenerator.INSTANCE.nextId(), "group-b");
    IdpGroupPO groupA = createIdpGroupPO(RandomIdGenerator.INSTANCE.nextId(), "group-a");
    groupMetaService.createGroup(groupB);
    groupMetaService.createGroup(groupA);
    groupMetaService.addUsersToGroup(
        Arrays.asList(
            createIdpGroupUserRelPO(RandomIdGenerator.INSTANCE.nextId(), groupB, user),
            createIdpGroupUserRelPO(RandomIdGenerator.INSTANCE.nextId(), groupA, user)));

    assertEquals(
        Arrays.asList("group-a", "group-b"), groupMetaService.listGroupNames(user.getUserId()));
  }

  private IdpUserPO createIdpUserPO(Long id, String userName, String passwordHash) {
    return IdpUserPO.builder()
        .withUserId(id)
        .withUserName(userName)
        .withPasswordHash(passwordHash)
        .withAuditInfo(AUDIT_INFO_JSON)
        .withCurrentVersion(0L)
        .withLastVersion(0L)
        .withDeletedAt(0L)
        .build();
  }

  private IdpGroupPO createIdpGroupPO(Long id, String groupName) {
    return IdpGroupPO.builder()
        .withGroupId(id)
        .withGroupName(groupName)
        .withAuditInfo(AUDIT_INFO_JSON)
        .withCurrentVersion(0L)
        .withLastVersion(0L)
        .withDeletedAt(0L)
        .build();
  }

  private IdpGroupUserRelPO createIdpGroupUserRelPO(Long id, IdpGroupPO group, IdpUserPO user) {
    return IdpGroupUserRelPO.builder()
        .withId(id)
        .withGroupId(group.getGroupId())
        .withUserId(user.getUserId())
        .withAuditInfo(AUDIT_INFO_JSON)
        .withCurrentVersion(0L)
        .withLastVersion(0L)
        .withDeletedAt(0L)
        .build();
  }

  private long futureTimestamp() {
    return Instant.now().toEpochMilli() + 1000;
  }

  private int countIdpGroups() {
    return countRows("idp_group_meta");
  }

  private int countIdpGroupUserRels() {
    return countRows("idp_group_user_rel");
  }

  private int countRows(String tableName) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM " + tableName)) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new RuntimeException("Doesn't contain data");
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }
}
