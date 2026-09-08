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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;

class TestRoleMembershipWrites extends TestJDBCBackend {
  private static final String METALAKE = "membership_metalake";
  private static final String CATALOG = "catalog";

  @TestTemplate
  void testGrantRejectsRoleDeletedAfterObservation() throws Exception {
    initialize();
    RoleEntity retained = role(METALAKE, "retained", true);
    for (boolean group : List.of(false, true)) {
      RoleEntity deleted = role(METALAKE, "deleted_" + group, true);
      long id = RandomIdGenerator.INSTANCE.nextId();
      insertPrincipal(group, id, List.of(retained), false);
      long version = version(group, id);
      assertThrows(
          NoSuchRoleException.class,
          () ->
              updatePrincipal(
                  group,
                  List.of(retained, deleted),
                  () -> RoleMetaService.getInstance().deleteRole(deleted.nameIdentifier())));
      assertEquals(version, version(group, id));
      assertEquals(1, memberships(group, id));
    }
  }

  @TestTemplate
  void testInvalidRoleBatchRollsBackInsertOverwriteAndUpdate() throws Exception {
    initialize();
    String otherMetalake = "other_membership_metalake";
    createAndInsertMakeLake(otherMetalake);
    createAndInsertCatalog(otherMetalake, CATALOG);
    RoleEntity retained = role(METALAKE, "retained", true);
    RoleEntity valid = role(METALAKE, "valid", true);
    RoleEntity missing = role(METALAKE, "missing", false);
    RoleEntity foreign = role(otherMetalake, "foreign", true);
    RoleEntity deleted = role(METALAKE, "deleted", true);
    RoleMetaService.getInstance().deleteRole(deleted.nameIdentifier());
    RoleEntity replaced = role(METALAKE, "recreated", true);
    RoleMetaService.getInstance().deleteRole(replaced.nameIdentifier());
    RoleEntity replacement = role(METALAKE, "recreated", true);
    for (boolean group : List.of(false, true)) {
      long id = RandomIdGenerator.INSTANCE.nextId();
      for (RoleEntity invalid : List.of(missing, foreign, deleted, replaced)) {
        assertThrows(
            NoSuchRoleException.class,
            () -> insertPrincipal(group, id, List.of(valid, invalid), false));
        assertFalse(backend.exists(identifier(group), type(group)));
        assertEquals(0, memberships(group, id));
      }
      insertPrincipal(group, id, List.of(retained), false);
      long version = version(group, id);
      for (RoleEntity invalid : List.of(missing, foreign, deleted, replaced)) {
        assertThrows(
            NoSuchRoleException.class,
            () -> insertPrincipal(group, id, List.of(valid, invalid), true));
        assertEquals(version, version(group, id));
        assertEquals(1, memberships(group, id));
        assertEquals(
            1,
            queryLong(
                "SELECT COUNT(*) FROM "
                    + relationTable(group)
                    + " WHERE "
                    + principalColumn(group)
                    + " = "
                    + id
                    + " AND role_id = "
                    + retained.id()
                    + " AND deleted_at = 0"));
        assertThrows(
            NoSuchRoleException.class,
            () -> updatePrincipal(group, List.of(valid, invalid), () -> {}));
        assertEquals(version, version(group, id));
        assertEquals(1, memberships(group, id));
        assertEquals(
            1,
            queryLong(
                "SELECT COUNT(*) FROM "
                    + relationTable(group)
                    + " WHERE "
                    + principalColumn(group)
                    + " = "
                    + id
                    + " AND role_id = "
                    + retained.id()
                    + " AND deleted_at = 0"));
      }
      // The old ID must fail, but a fresh reference to the replacement remains usable.
      updatePrincipal(group, List.of(replacement), () -> {});
      assertEquals(1, memberships(group, id));
    }
  }

  @TestTemplate
  void testValidOverwriteGrantAndRevokeRemainIdempotent() throws Exception {
    initialize();
    RoleEntity first = role(METALAKE, "first", true);
    RoleEntity second = role(METALAKE, "second", true);
    for (boolean group : List.of(false, true)) {
      long id = RandomIdGenerator.INSTANCE.nextId();
      insertPrincipal(group, id, List.of(first), false);
      insertPrincipal(group, id, List.of(second), true);
      assertEquals(1, memberships(group, id));
      updatePrincipal(group, List.of(second, first), () -> {});
      updatePrincipal(group, List.of(second, first), () -> {});
      assertEquals(2, memberships(group, id));
      updatePrincipal(group, List.of(), () -> {});
      updatePrincipal(group, List.of(), () -> {});
      assertEquals(0, memberships(group, id));
    }
  }

  @TestTemplate
  void testGrantWaitsForUncommittedRoleDelete() throws Exception {
    initialize();
    for (boolean group : List.of(false, true)) {
      RoleEntity role = role(METALAKE, "delete_first_" + group, true);
      long id = RandomIdGenerator.INSTANCE.nextId();
      insertPrincipal(group, id, List.of(), false);
      long version = version(group, id);
      Throwable failure =
          whileTransactionHeld(
              () -> RoleMetaService.getInstance().deleteRole(role.nameIdentifier()),
              () -> updatePrincipal(group, List.of(role), () -> {}));
      Assertions.assertInstanceOf(NoSuchRoleException.class, failure);
      assertEquals(version, version(group, id));
      assertEquals(0, memberships(group, id));
    }
  }

  @TestTemplate
  void testRoleDeleteWaitsForGrantAndCleansMembership() throws Exception {
    initialize();
    for (boolean group : List.of(false, true)) {
      RoleEntity role = role(METALAKE, "grant_first_" + group, true);
      long id = RandomIdGenerator.INSTANCE.nextId();
      insertPrincipal(group, id, List.of(), false);
      assertNull(
          whileTransactionHeld(
              () -> updatePrincipal(group, List.of(role), () -> {}),
              () -> RoleMetaService.getInstance().deleteRole(role.nameIdentifier())));
      assertEquals(0, memberships(group, id));
      assertEquals(
          1,
          queryLong(
              "SELECT COUNT(*) FROM "
                  + relationTable(group)
                  + " WHERE "
                  + principalColumn(group)
                  + " = "
                  + id));
    }
  }

  @TestTemplate
  void testIndependentGrantsShareRoleLock() throws Exception {
    initialize();
    RoleEntity role = role(METALAKE, "shared", true);
    long userId = RandomIdGenerator.INSTANCE.nextId();
    long groupId = RandomIdGenerator.INSTANCE.nextId();
    insertPrincipal(false, userId, List.of(), false);
    insertPrincipal(true, groupId, List.of(), false);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);
    SessionUtils.beginTransaction();
    try {
      updatePrincipal(false, List.of(role), () -> {});
      Future<?> grant =
          executor.submit(
              () -> {
                started.countDown();
                updatePrincipal(true, List.of(role), () -> {});
                return null;
              });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      String database =
          SqlSessionFactoryHelper.getInstance()
              .getSqlSessionFactory()
              .getConfiguration()
              .getDatabaseId();
      if ("h2".equalsIgnoreCase(database)) {
        assertThrows(TimeoutException.class, () -> grant.get(500, TimeUnit.MILLISECONDS));
      } else {
        // Independent principals can commit grants while the first transaction still holds its
        // lock.
        grant.get(10, TimeUnit.SECONDS);
      }
      SessionUtils.commitTransaction();
      grant.get(10, TimeUnit.SECONDS);
      assertEquals(1, memberships(false, userId));
      assertEquals(1, memberships(true, groupId));
    } finally {
      SessionUtils.rollbackTransaction();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @TestTemplate
  void testMetalakeCascadeWaitsForGrant() throws Exception {
    for (boolean group : List.of(false, true)) {
      initialize();
      RoleEntity role = role(METALAKE, "cascade", true);
      long id = RandomIdGenerator.INSTANCE.nextId();
      insertPrincipal(group, id, List.of(), false);
      assertNull(
          whileTransactionHeld(
              () -> updatePrincipal(group, List.of(role), () -> {}),
              () -> backend.delete(NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE, true)));
      assertEquals(0, memberships(group, id));
      assertFalse(backend.exists(identifier(group), type(group)));
    }
  }

  @TestTemplate
  void testRoleDeleteWaitsForInsertAndOverwrite() throws Exception {
    initialize();
    for (boolean group : List.of(false, true)) {
      long id = RandomIdGenerator.INSTANCE.nextId();
      for (boolean overwrite : List.of(false, true)) {
        RoleEntity role = role(METALAKE, "insert_" + group + "_" + overwrite, true);
        assertNull(
            whileTransactionHeld(
                () -> insertPrincipal(group, id, List.of(role), overwrite),
                () -> RoleMetaService.getInstance().deleteRole(role.nameIdentifier())));
        assertEquals(0, memberships(group, id));
      }
    }
  }

  @TestTemplate
  void testGrantRejectsMetalakeDeletedAfterObservation() throws Exception {
    for (boolean group : List.of(false, true)) {
      initialize();
      RoleEntity role = role(METALAKE, "deleted_metalake", true);
      long id = RandomIdGenerator.INSTANCE.nextId();
      insertPrincipal(group, id, List.of(), false);
      assertThrows(
          NoSuchEntityException.class,
          () ->
              updatePrincipal(
                  group,
                  List.of(role),
                  () ->
                      Assertions.assertDoesNotThrow(
                          () ->
                              backend.delete(
                                  NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE, true))));
      assertEquals(0, memberships(group, id));
      assertFalse(backend.exists(identifier(group), type(group)));
    }
  }

  private void initialize() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, CATALOG);
  }

  private RoleEntity role(String metalake, String name, boolean insert) throws IOException {
    RoleEntity role =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalake),
            name,
            AUDIT_INFO,
            CATALOG);
    if (insert) {
      RoleMetaService.getInstance().insertRole(role, false);
    }
    return role;
  }

  private UserEntity user(long id, List<RoleEntity> roles) {
    return createUserEntity(
        id,
        AuthorizationUtils.ofUserNamespace(METALAKE),
        "user",
        AUDIT_INFO,
        roles.stream().map(RoleEntity::name).collect(Collectors.toList()),
        roles.stream().map(RoleEntity::id).collect(Collectors.toList()));
  }

  private GroupEntity group(long id, List<RoleEntity> roles) {
    return createGroupEntity(
        id,
        AuthorizationUtils.ofGroupNamespace(METALAKE),
        "group",
        AUDIT_INFO,
        roles.stream().map(RoleEntity::name).collect(Collectors.toList()),
        roles.stream().map(RoleEntity::id).collect(Collectors.toList()));
  }

  private void insertPrincipal(boolean group, long id, List<RoleEntity> roles, boolean overwrite)
      throws IOException {
    if (group) {
      GroupMetaService.getInstance().insertGroup(group(id, roles), overwrite);
    } else {
      UserMetaService.getInstance().insertUser(user(id, roles), overwrite);
    }
  }

  private void updatePrincipal(boolean group, List<RoleEntity> roles, Runnable beforeWrite)
      throws IOException {
    if (group) {
      GroupMetaService.getInstance()
          .updateGroup(
              identifier(true),
              (GroupEntity old) -> {
                beforeWrite.run();
                return group(old.id(), roles);
              });
    } else {
      UserMetaService.getInstance()
          .updateUser(
              identifier(false),
              (UserEntity old) -> {
                beforeWrite.run();
                return user(old.id(), roles);
              });
    }
  }

  private NameIdentifier identifier(boolean group) {
    return group
        ? AuthorizationUtils.ofGroup(METALAKE, "group")
        : AuthorizationUtils.ofUser(METALAKE, "user");
  }

  private Entity.EntityType type(boolean group) {
    return group ? Entity.EntityType.GROUP : Entity.EntityType.USER;
  }

  private String relationTable(boolean group) {
    return group ? "group_role_rel" : "user_role_rel";
  }

  private String principalColumn(boolean group) {
    return group ? "group_id" : "user_id";
  }

  private long memberships(boolean group, long id) throws Exception {
    return queryLong(
        "SELECT COUNT(*) FROM "
            + relationTable(group)
            + " WHERE "
            + principalColumn(group)
            + " = "
            + id
            + " AND deleted_at = 0");
  }

  private long version(boolean group, long id) throws Exception {
    return queryLong(
        "SELECT current_version FROM "
            + (group ? "group_meta" : "user_meta")
            + " WHERE "
            + principalColumn(group)
            + " = "
            + id
            + " AND deleted_at = 0");
  }

  private long queryLong(String sql) throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Statement statement = session.getConnection().createStatement();
        ResultSet rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private Throwable whileTransactionHeld(Executable holder, Executable contender) throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);
    SessionUtils.beginTransaction();
    try {
      Assertions.assertDoesNotThrow(holder);
      Future<Throwable> result =
          executor.submit(
              () -> {
                started.countDown();
                try {
                  contender.execute();
                  return null;
                } catch (Throwable failure) {
                  return failure;
                }
              });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> result.get(500, TimeUnit.MILLISECONDS));
      SessionUtils.commitTransaction();
      return result.get(10, TimeUnit.SECONDS);
    } finally {
      SessionUtils.rollbackTransaction();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
