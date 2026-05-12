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
import org.junit.jupiter.api.Test;

public class TestIdpUserMetaMapper extends IdpMapperTestBase {

  @Test
  void testInsertIdpUserAndSelectIdpUser() {
    IdpUserPO firstUser = insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);

    assertEquals(firstUser, idpUserMetaMapper.selectIdpUser("alice"));
    assertNull(idpUserMetaMapper.selectIdpUser("unknown"));
  }

  @Test
  void testSelectIdpUsers() {
    IdpUserPO firstUser = insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    IdpUserPO secondUser = insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);

    List<IdpUserPO> users = idpUserMetaMapper.selectIdpUsers(List.of("bob", "alice"));
    users.sort(Comparator.comparing(IdpUserPO::getUserId));
    assertIterableEquals(List.of(firstUser, secondUser), users);
    assertTrue(idpUserMetaMapper.selectIdpUsers(List.of()).isEmpty());
    assertTrue(idpUserMetaMapper.selectIdpUsers(null).isEmpty());
  }

  @Test
  void testUpdateIdpUserPassword() {
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);

    assertEquals(1, idpUserMetaMapper.updateIdpUserPassword(1L, "hash-a-2", 1L, 2L, 2L));
    assertEquals("hash-a-2", idpUserMetaMapper.selectIdpUser("alice").getPasswordHash());
    assertEquals(2L, idpUserMetaMapper.selectIdpUser("alice").getCurrentVersion());
    assertEquals(2L, idpUserMetaMapper.selectIdpUser("alice").getLastVersion());
  }

  @Test
  void testUpdateIdpUserPasswordReturnsZeroForVersionMismatch() {
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);

    assertEquals(0, idpUserMetaMapper.updateIdpUserPassword(1L, "hash-a-2", 2L, 3L, 3L));
    assertEquals("hash-a", idpUserMetaMapper.selectIdpUser("alice").getPasswordHash());
    assertEquals(1L, idpUserMetaMapper.selectIdpUser("alice").getCurrentVersion());
    assertEquals(0L, idpUserMetaMapper.selectIdpUser("alice").getLastVersion());
  }

  @Test
  void testSoftDeleteIdpUser() {
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);

    idpUserMetaMapper.softDeleteIdpUser(1L, 100L);
    assertNull(idpUserMetaMapper.selectIdpUser("alice"));
    assertEquals(100L, queryLongValue("idp_user_meta", "deleted_at", "user_id", 1L));
    assertEquals(2L, queryLongValue("idp_user_meta", "current_version", "user_id", 1L));
    assertEquals(1L, queryLongValue("idp_user_meta", "last_version", "user_id", 1L));
  }

  @Test
  void testDeleteIdpUserMetasByLegacyTimeline() {
    insertUser(1L, "legacy-user", "hash", 1L, 0L, 10L);
    insertUser(2L, "new-user", "hash", 1L, 0L, 30L);
    insertUser(3L, "active-user", "hash", 1L, 0L, 0L);

    assertEquals(1, idpUserMetaMapper.deleteIdpUserMetasByLegacyTimeline(20L, 10));
    assertEquals(0, countRows("idp_user_meta", "user_id", 1L));
    assertEquals(1, countRows("idp_user_meta", "user_id", 2L));
    assertEquals(1, countRows("idp_user_meta", "user_id", 3L));
  }
}
