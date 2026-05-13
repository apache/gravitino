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

package org.apache.gravitino.storage.relational.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.junit.jupiter.api.TestTemplate;

public class TestIdpGroupUserRelMapper extends IdpMapperTestBase {

  @TestTemplate
  void testBatchInsertIdpGroupUsersAndSelectGroupNamesByUserId() {
    insertGroup(10L, "dev", 1L, 0L, 0L);
    insertGroup(20L, "ops", 1L, 0L, 0L);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertRelation(100L, 10L, 1L, 1L, 0L, 0L);
    insertRelation(101L, 20L, 1L, 1L, 0L, 0L);

    assertIterableEquals(List.of("dev", "ops"), idpGroupUserRelMapper.selectGroupNamesByUserId(1L));
    assertTrue(idpGroupUserRelMapper.selectGroupNamesByUserId(999L).isEmpty());
  }

  @TestTemplate
  void testSelectUserNamesByGroupId() {
    insertGroup(10L, "dev", 1L, 0L, 0L);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);
    insertRelation(100L, 10L, 1L, 1L, 0L, 0L);
    insertRelation(101L, 10L, 2L, 1L, 0L, 0L);

    assertIterableEquals(
        List.of("alice", "bob"), idpGroupUserRelMapper.selectUserNamesByGroupId(10L));
    assertTrue(idpGroupUserRelMapper.selectUserNamesByGroupId(999L).isEmpty());
  }

  @TestTemplate
  void testSelectRelatedUserIds() {
    insertGroup(10L, "dev", 1L, 0L, 0L);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);
    insertUser(3L, "carol", "hash-c", 1L, 0L, 0L);
    insertRelation(100L, 10L, 1L, 1L, 0L, 0L);
    insertRelation(101L, 10L, 2L, 1L, 0L, 0L);

    List<Long> relatedUserIds =
        idpGroupUserRelMapper.selectRelatedUserIds(10L, List.of(2L, 3L, 1L));
    relatedUserIds.sort(Long::compareTo);
    assertIterableEquals(List.of(1L, 2L), relatedUserIds);
    assertTrue(idpGroupUserRelMapper.selectRelatedUserIds(10L, List.of()).isEmpty());
    assertTrue(idpGroupUserRelMapper.selectRelatedUserIds(10L, null).isEmpty());
  }

  @TestTemplate
  void testSoftDeleteIdpGroupUsers() {
    insertGroup(10L, "dev", 1L, 0L, 0L);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);
    IdpGroupUserRelPO firstRelation = insertRelation(100L, 10L, 1L, 1L, 0L, 0L);
    insertRelation(101L, 10L, 2L, 1L, 0L, 0L);

    idpGroupUserRelMapper.softDeleteIdpGroupUsers(10L, List.of(1L));
    assertIterableEquals(List.of("bob"), idpGroupUserRelMapper.selectUserNamesByGroupId(10L));
    assertTrue(
        queryLongValue("idp_group_user_rel", "deleted_at", "id", firstRelation.getId()) > 0L);
    assertEquals(
        2L, queryLongValue("idp_group_user_rel", "current_version", "id", firstRelation.getId()));
    assertEquals(
        1L, queryLongValue("idp_group_user_rel", "last_version", "id", firstRelation.getId()));
  }

  @TestTemplate
  void testSoftDeleteGroupUsersByUserId() {
    insertGroup(10L, "dev", 1L, 0L, 0L);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);
    insertRelation(100L, 10L, 1L, 1L, 0L, 0L);
    IdpGroupUserRelPO secondRelation = insertRelation(101L, 10L, 2L, 1L, 0L, 0L);

    idpGroupUserRelMapper.softDeleteGroupUsersByUserId(2L);
    assertIterableEquals(List.of("alice"), idpGroupUserRelMapper.selectUserNamesByGroupId(10L));
    assertTrue(
        queryLongValue("idp_group_user_rel", "deleted_at", "id", secondRelation.getId()) > 0L);
  }

  @TestTemplate
  void testSoftDeleteGroupUsersByGroupId() {
    insertGroup(20L, "ops", 1L, 0L, 0L);
    insertUser(3L, "carol", "hash-c", 1L, 0L, 0L);
    IdpGroupUserRelPO thirdRelation = insertRelation(102L, 20L, 3L, 1L, 0L, 0L);

    idpGroupUserRelMapper.softDeleteGroupUsersByGroupId(20L);
    assertTrue(idpGroupUserRelMapper.selectUserNamesByGroupId(20L).isEmpty());
    assertTrue(
        queryLongValue("idp_group_user_rel", "deleted_at", "id", thirdRelation.getId()) > 0L);
  }

  @TestTemplate
  void testDeleteIdpGroupUserRelMetasByLegacyTimeline() {
    insertRelation(100L, 10L, 1L, 1L, 0L, 10L);
    insertRelation(101L, 20L, 2L, 1L, 0L, 30L);
    insertRelation(102L, 30L, 3L, 1L, 0L, 0L);

    assertEquals(1, idpGroupUserRelMapper.deleteIdpGroupUserRelMetasByLegacyTimeline(20L, 10));

    assertEquals(0, countRows("idp_group_user_rel", "id", 100L));
    assertEquals(1, countRows("idp_group_user_rel", "id", 101L));
    assertEquals(1, countRows("idp_group_user_rel", "id", 102L));
  }
}
