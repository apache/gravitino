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

package org.apache.gravitino.idp.storage.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.exceptions.PersistenceException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("gravitino-docker-test")
class TestIdpUserMetaStorage extends AbstractIdpMetaStorageTest {

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testInsertIdpUserAndSelectIdpUser(String type) throws IOException {
    init(type);
    IdpUserPO firstUser = insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);

    assertEquals(firstUser, idpUserMetaMapper.selectIdpUser("alice"));
    assertNull(idpUserMetaMapper.selectIdpUser("unknown"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSelectIdpUsers(String type) throws IOException {
    init(type);
    IdpUserPO firstUser = insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    IdpUserPO secondUser = insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);

    List<IdpUserPO> users = idpUserMetaMapper.selectIdpUsers(List.of("bob", "alice"));
    users.sort(Comparator.comparing(IdpUserPO::getUserId));
    assertIterableEquals(List.of(firstUser, secondUser), users);
    List<IdpUserPO> usersWithEmptyFilter = idpUserMetaMapper.selectIdpUsers(List.of());
    usersWithEmptyFilter.sort(Comparator.comparing(IdpUserPO::getUserId));
    assertIterableEquals(List.of(firstUser, secondUser), usersWithEmptyFilter);
    assertThrows(PersistenceException.class, () -> idpUserMetaMapper.selectIdpUsers(null));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSelectIdpUsersIgnoresDeletedUsers(String type) throws IOException {
    init(type);
    IdpUserPO activeUser = insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertUser(2L, "bob", "hash-b", 1L, 0L, 10L);

    assertIterableEquals(
        List.of(activeUser), idpUserMetaMapper.selectIdpUsers(List.of("alice", "bob")));
    assertNull(idpUserMetaMapper.selectIdpUser("bob"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testUpdateIdpUserPassword(String type) throws IOException {
    init(type);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);

    assertEquals(1, idpUserMetaMapper.updateIdpUserPassword(1L, "hash-a-2"));
    assertEquals("hash-a-2", idpUserMetaMapper.selectIdpUser("alice").getPasswordHash());
    assertEquals(1L, idpUserMetaMapper.selectIdpUser("alice").getCurrentVersion());
    assertEquals(0L, idpUserMetaMapper.selectIdpUser("alice").getLastVersion());
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testUpdateIdpUserPasswordKeepsVersionsUnchanged(String type) throws IOException {
    init(type);
    insertUser(1L, "alice", "hash-a", 3L, 2L, 0L);

    assertEquals(1, idpUserMetaMapper.updateIdpUserPassword(1L, "hash-a-2"));
    assertEquals("hash-a-2", idpUserMetaMapper.selectIdpUser("alice").getPasswordHash());
    assertEquals(3L, idpUserMetaMapper.selectIdpUser("alice").getCurrentVersion());
    assertEquals(2L, idpUserMetaMapper.selectIdpUser("alice").getLastVersion());
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteIdpUser(String type) throws IOException {
    init(type);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);

    assertEquals(1, idpUserMetaMapper.softDeleteIdpUser(1L));
    assertNull(idpUserMetaMapper.selectIdpUser("alice"));
    assertTrue(queryLongValue("idp_user_meta", "deleted_at", "user_id", 1L) > 0L);
    assertEquals(1L, queryLongValue("idp_user_meta", "current_version", "user_id", 1L));
    assertEquals(0L, queryLongValue("idp_user_meta", "last_version", "user_id", 1L));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteIdpUserMetasByLegacyTimeline(String type) throws IOException {
    init(type);
    insertUser(1L, "legacy-user", "hash", 1L, 0L, 10L);
    insertUser(2L, "new-user", "hash", 1L, 0L, 30L);
    insertUser(3L, "active-user", "hash", 1L, 0L, 0L);

    assertEquals(1, idpUserMetaMapper.deleteIdpUserMetasByLegacyTimeline(20L, 10));
    assertEquals(0, countRows("idp_user_meta", "user_id", 1L));
    assertEquals(1, countRows("idp_user_meta", "user_id", 2L));
    assertEquals(1, countRows("idp_user_meta", "user_id", 3L));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testRestart(String type) throws IOException {
    init(type);
    IdpUserPO expectedActiveUser = insertUser(1L, "alice", "hash-a", 3L, 2L, 0L);
    insertUser(2L, "bob", "hash-b", 1L, 0L, 10L);
    assertEquals(1, idpUserMetaMapper.updateIdpUserPassword(1L, "hash-a-2"));

    expectedActiveUser =
        IdpUserPO.builder()
            .withUserId(expectedActiveUser.getUserId())
            .withUserName(expectedActiveUser.getUserName())
            .withPasswordHash("hash-a-2")
            .withCurrentVersion(expectedActiveUser.getCurrentVersion())
            .withLastVersion(expectedActiveUser.getLastVersion())
            .withDeletedAt(expectedActiveUser.getDeletedAt())
            .build();

    assertPersistedUsers(expectedActiveUser);

    restartBackend();

    assertPersistedUsers(expectedActiveUser);
  }

  private void assertPersistedUsers(IdpUserPO expectedActiveUser) {
    assertTrue(
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .hasMapper(IdpUserMetaMapper.class));

    assertEquals(expectedActiveUser, idpUserMetaMapper.selectIdpUser("alice"));
    assertNull(idpUserMetaMapper.selectIdpUser("bob"));

    List<IdpUserPO> users = idpUserMetaMapper.selectIdpUsers(List.of("bob", "alice"));
    users.sort(Comparator.comparing(IdpUserPO::getUserId));
    assertIterableEquals(List.of(expectedActiveUser), users);
  }
}
