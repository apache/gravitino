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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.List;
import org.apache.gravitino.storage.relational.po.IdpUserPO;

interface IdpUserMetaMapperTest {
  IdpMapperTestBase testBase();

  default void testInsertIdpUserAndSelectIdpUser() {
    IdpMapperTestBase testBase = testBase();
    IdpUserPO firstUser = testBase.insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);

    assertEquals(firstUser, testBase.idpUserMetaMapper.selectIdpUser("alice"));
    assertNull(testBase.idpUserMetaMapper.selectIdpUser("unknown"));
  }

  default void testSelectIdpUsers() {
    IdpMapperTestBase testBase = testBase();
    IdpUserPO firstUser = testBase.insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    IdpUserPO secondUser = testBase.insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);

    List<IdpUserPO> users = testBase.idpUserMetaMapper.selectIdpUsers(List.of("bob", "alice"));
    users.sort(Comparator.comparing(IdpUserPO::getUserId));
    assertIterableEquals(List.of(firstUser, secondUser), users);
    assertTrue(testBase.idpUserMetaMapper.selectIdpUsers(List.of()).isEmpty());
    assertTrue(testBase.idpUserMetaMapper.selectIdpUsers(null).isEmpty());
  }

  default void testSelectIdpUsersIgnoresDeletedUsers() {
    IdpMapperTestBase testBase = testBase();
    IdpUserPO activeUser = testBase.insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    testBase.insertUser(2L, "bob", "hash-b", 1L, 0L, 10L);

    assertIterableEquals(
        List.of(activeUser), testBase.idpUserMetaMapper.selectIdpUsers(List.of("alice", "bob")));
    assertNull(testBase.idpUserMetaMapper.selectIdpUser("bob"));
  }

  default void testUpdateIdpUserPassword() {
    IdpMapperTestBase testBase = testBase();
    testBase.insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);

    assertEquals(1, testBase.idpUserMetaMapper.updateIdpUserPassword(1L, "hash-a-2"));
    assertEquals("hash-a-2", testBase.idpUserMetaMapper.selectIdpUser("alice").getPasswordHash());
    assertEquals(1L, testBase.idpUserMetaMapper.selectIdpUser("alice").getCurrentVersion());
    assertEquals(0L, testBase.idpUserMetaMapper.selectIdpUser("alice").getLastVersion());
  }

  default void testUpdateIdpUserPasswordKeepsVersionsUnchanged() {
    IdpMapperTestBase testBase = testBase();
    testBase.insertUser(1L, "alice", "hash-a", 3L, 2L, 0L);

    assertEquals(1, testBase.idpUserMetaMapper.updateIdpUserPassword(1L, "hash-a-2"));
    assertEquals("hash-a-2", testBase.idpUserMetaMapper.selectIdpUser("alice").getPasswordHash());
    assertEquals(3L, testBase.idpUserMetaMapper.selectIdpUser("alice").getCurrentVersion());
    assertEquals(2L, testBase.idpUserMetaMapper.selectIdpUser("alice").getLastVersion());
  }

  default void testUpdateIdpUserPasswordReturnsZeroForDeletedUser() {
    IdpMapperTestBase testBase = testBase();
    testBase.insertUser(1L, "alice", "hash-a", 1L, 0L, 10L);

    assertEquals(0, testBase.idpUserMetaMapper.updateIdpUserPassword(1L, "hash-a-2"));
    assertEquals(
        1L, testBase.queryLongValueInMapperTest("idp_user_meta", "current_version", "user_id", 1L));
    assertEquals(
        0L, testBase.queryLongValueInMapperTest("idp_user_meta", "last_version", "user_id", 1L));
    assertEquals(
        10L, testBase.queryLongValueInMapperTest("idp_user_meta", "deleted_at", "user_id", 1L));
  }

  default void testSoftDeleteIdpUser() {
    IdpMapperTestBase testBase = testBase();
    testBase.insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);

    assertEquals(1, testBase.idpUserMetaMapper.softDeleteIdpUser(1L));
    assertNull(testBase.idpUserMetaMapper.selectIdpUser("alice"));
    assertTrue(
        testBase.queryLongValueInMapperTest("idp_user_meta", "deleted_at", "user_id", 1L) > 0L);
    assertEquals(
        1L, testBase.queryLongValueInMapperTest("idp_user_meta", "current_version", "user_id", 1L));
    assertEquals(
        0L, testBase.queryLongValueInMapperTest("idp_user_meta", "last_version", "user_id", 1L));
  }

  default void testSoftDeleteIdpUserReturnsZeroForDeletedUser() {
    IdpMapperTestBase testBase = testBase();
    testBase.insertUser(1L, "alice", "hash-a", 1L, 0L, 10L);

    assertEquals(0, testBase.idpUserMetaMapper.softDeleteIdpUser(1L));
    assertEquals(
        10L, testBase.queryLongValueInMapperTest("idp_user_meta", "deleted_at", "user_id", 1L));
    assertEquals(
        1L, testBase.queryLongValueInMapperTest("idp_user_meta", "current_version", "user_id", 1L));
    assertEquals(
        0L, testBase.queryLongValueInMapperTest("idp_user_meta", "last_version", "user_id", 1L));
  }

  default void testDeleteIdpUserMetasByLegacyTimeline() {
    IdpMapperTestBase testBase = testBase();
    testBase.insertUser(1L, "legacy-user", "hash", 1L, 0L, 10L);
    testBase.insertUser(2L, "new-user", "hash", 1L, 0L, 30L);
    testBase.insertUser(3L, "active-user", "hash", 1L, 0L, 0L);

    assertEquals(1, testBase.idpUserMetaMapper.deleteIdpUserMetasByLegacyTimeline(20L, 10));
    assertEquals(0, testBase.countRowsInMapperTest("idp_user_meta", "user_id", 1L));
    assertEquals(1, testBase.countRowsInMapperTest("idp_user_meta", "user_id", 2L));
    assertEquals(1, testBase.countRowsInMapperTest("idp_user_meta", "user_id", 3L));
  }
}
