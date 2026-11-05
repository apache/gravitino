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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;

/**
 * Races between owner assignment and deletion of the owned object or owner principal, and between
 * two assignments on the same object. Every scenario is driven by a real transaction held open on
 * one thread while the contender runs on another, so the assertions describe what two Gravitino
 * servers sharing one database would observe.
 */
class TestOwnerAssignmentWrites extends TestJDBCBackend {
  private static final String METALAKE = "owner_write_metalake";

  @TestTemplate
  void testAssignmentWaitsForUncommittedPrincipalDeleteAndFails() throws Exception {
    createAndInsertMakeLake(METALAKE);
    for (boolean group : List.of(false, true)) {
      CatalogEntity owned = createAndInsertCatalog(METALAKE, "owned_" + group);
      NameIdentifier principal = insertPrincipal(group, "deleted_" + group);
      long id = principalId(group, principal);
      Throwable failure =
          whileTransactionHeld(
              () -> deletePrincipal(group, principal),
              () -> setOwner(owned, principal, type(group)));
      assertInstanceOf(NoSuchEntityException.class, failure);
      assertFalse(owner(owned).isPresent());
      assertEquals(0, liveOwnerRows(id));
    }
  }

  @TestTemplate
  void testAssignmentWaitsForUncommittedOwnedObjectDeleteAndFails() throws Exception {
    createAndInsertMakeLake(METALAKE);
    CatalogEntity owned = createAndInsertCatalog(METALAKE, "owned");
    NameIdentifier principal = insertPrincipal(false, "owner");
    Throwable failure =
        whileTransactionHeld(
            () ->
                assertTrue(
                    CatalogMetaService.getInstance().deleteCatalog(owned.nameIdentifier(), false)),
            () -> setOwner(owned, principal, Entity.EntityType.USER));
    assertInstanceOf(NoSuchEntityException.class, failure);
    assertEquals(0, liveOwnerRowsForObject(owned));
  }

  @TestTemplate
  void testPrincipalDeleteWaitsForUncommittedAssignmentAndCleansIt() throws Exception {
    createAndInsertMakeLake(METALAKE);
    for (boolean group : List.of(false, true)) {
      CatalogEntity owned = createAndInsertCatalog(METALAKE, "owned_" + group);
      NameIdentifier principal = insertPrincipal(group, "deleted_" + group);
      long id = principalId(group, principal);
      assertNull(
          whileTransactionHeld(
              () -> setOwner(owned, principal, type(group)),
              () -> deletePrincipal(group, principal)));
      assertFalse(owner(owned).isPresent());
      assertEquals(0, liveOwnerRows(id));
    }
  }

  @TestTemplate
  void testAssignmentRejectsSameNameReplacementObservedByStaleId() throws Exception {
    createAndInsertMakeLake(METALAKE);
    for (boolean group : List.of(false, true)) {
      CatalogEntity owned = createAndInsertCatalog(METALAKE, "owned_" + group);
      NameIdentifier principal = insertPrincipal(group, "recreated_" + group);
      long staleId = principalId(group, principal);
      Throwable failure =
          whileTransactionHeld(
              () -> {
                deletePrincipal(group, principal);
                insertPrincipal(group, principal.name());
              },
              () -> setOwner(owned, principal, type(group)));
      assertInstanceOf(NoSuchEntityException.class, failure);
      assertEquals(0, liveOwnerRows(staleId));
      assertFalse(owner(owned).isPresent());

      // A fresh lookup resolves the replacement and succeeds.
      setOwner(owned, principal, type(group));
      assertEquals(principal.name(), ownerName(owned));
      assertEquals(1, liveOwnerRows(principalId(group, principal)));
    }
  }

  @TestTemplate
  void testConcurrentInitialAssignmentsLeaveExactlyOneLiveOwner() throws Exception {
    createAndInsertMakeLake(METALAKE);
    CatalogEntity owned = createAndInsertCatalog(METALAKE, "owned");
    NameIdentifier first = insertPrincipal(false, "first");
    NameIdentifier second = insertPrincipal(true, "second");
    assertNull(
        whileTransactionHeld(
            () -> setOwner(owned, first, Entity.EntityType.USER),
            () -> setOwner(owned, second, Entity.EntityType.GROUP),
            true));
    assertEquals(1, liveOwnerRowsForObject(owned));
    assertEquals("second", ownerName(owned));
  }

  @TestTemplate
  void testConcurrentMetalakeAssignmentsSerializeOnTheMetalakeRow() throws Exception {
    createAndInsertMakeLake(METALAKE);
    NameIdentifier owned = NameIdentifier.of(METALAKE);
    NameIdentifier first = insertPrincipal(false, "first");
    NameIdentifier second = insertPrincipal(true, "second");
    assertNull(
        whileTransactionHeld(
            () ->
                OwnerMetaService.getInstance()
                    .setOwner(owned, Entity.EntityType.METALAKE, first, Entity.EntityType.USER),
            () ->
                OwnerMetaService.getInstance()
                    .setOwner(owned, Entity.EntityType.METALAKE, second, Entity.EntityType.GROUP)));
    long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(METALAKE);
    assertEquals(
        1,
        queryLong(
            "SELECT COUNT(*) FROM owner_meta WHERE metadata_object_id = "
                + metalakeId
                + " AND metadata_object_type = 'METALAKE' AND deleted_at = 0"));
    assertEquals(
        "second",
        assertInstanceOf(
                GroupEntity.class,
                OwnerMetaService.getInstance()
                    .getOwner(owned, Entity.EntityType.METALAKE)
                    .orElseThrow())
            .name());
  }

  @TestTemplate
  void testConcurrentReassignmentsSerializeOnTheExistingOwnerRow() throws Exception {
    createAndInsertMakeLake(METALAKE);
    CatalogEntity owned = createAndInsertCatalog(METALAKE, "owned");
    NameIdentifier initial = insertPrincipal(false, "initial");
    NameIdentifier first = insertPrincipal(false, "first");
    NameIdentifier second = insertPrincipal(true, "second");
    setOwner(owned, initial, Entity.EntityType.USER);
    assertNull(
        whileTransactionHeld(
            () -> setOwner(owned, first, Entity.EntityType.USER),
            () -> setOwner(owned, second, Entity.EntityType.GROUP),
            true));
    assertEquals(1, liveOwnerRowsForObject(owned));
    assertEquals("second", ownerName(owned));
  }

  @TestTemplate
  void testBatchAssignmentRollsBackWhenPrincipalIsDeleted() throws Exception {
    createAndInsertMakeLake(METALAKE);
    for (boolean group : List.of(false, true)) {
      NameIdentifier previous = insertPrincipal(false, "previous_" + group);
      NameIdentifier principal = insertPrincipal(group, "deleted_" + group);
      long id = principalId(group, principal);
      List<CatalogEntity> owned = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        CatalogEntity catalog = createAndInsertCatalog(METALAKE, "owned_" + group + "_" + i);
        owned.add(catalog);
        if (i > 0) {
          setOwner(catalog, previous, Entity.EntityType.USER);
        }
      }
      Throwable failure =
          whileTransactionHeld(
              () -> deletePrincipal(group, principal),
              () -> batchSetOwners(owned, principal, type(group)));
      assertInstanceOf(NoSuchEntityException.class, failure);
      assertFalse(SessionUtils.isInTransaction());
      assertFalse(owner(owned.get(0)).isPresent());
      assertEquals(previous.name(), ownerName(owned.get(1)));
      assertEquals(previous.name(), ownerName(owned.get(2)));
      assertEquals(0, liveOwnerRows(id));
    }
  }

  @TestTemplate
  void testBatchAssignmentSucceedsAfterPrincipalDeleteRollsBack() throws Exception {
    createAndInsertMakeLake(METALAKE);
    for (boolean group : List.of(false, true)) {
      NameIdentifier principal = insertPrincipal(group, "kept_" + group);
      List<CatalogEntity> owned =
          List.of(
              createAndInsertCatalog(METALAKE, "owned_" + group + "_0"),
              createAndInsertCatalog(METALAKE, "owned_" + group + "_1"));
      assertNull(
          whileTransactionHeld(
              () -> deletePrincipal(group, principal),
              () -> batchSetOwners(owned, principal, type(group)),
              () -> {},
              false));
      for (CatalogEntity catalog : owned) {
        assertEquals(principal.name(), ownerName(catalog));
      }
      assertEquals(2, liveOwnerRows(principalId(group, principal)));
    }
  }

  private void setOwner(CatalogEntity owned, NameIdentifier owner, Entity.EntityType ownerType) {
    OwnerMetaService.getInstance()
        .setOwner(owned.nameIdentifier(), Entity.EntityType.CATALOG, owner, ownerType);
  }

  private void batchSetOwners(
      List<CatalogEntity> owned, NameIdentifier owner, Entity.EntityType ownerType) {
    List<NameIdentifier> identifiers = new ArrayList<>();
    for (CatalogEntity catalog : owned) {
      identifiers.add(catalog.nameIdentifier());
    }
    OwnerMetaService.getInstance()
        .batchSetOwners(identifiers, Entity.EntityType.CATALOG, owner, ownerType);
  }

  private Optional<Entity> owner(CatalogEntity owned) {
    return OwnerMetaService.getInstance()
        .getOwner(owned.nameIdentifier(), Entity.EntityType.CATALOG);
  }

  private String ownerName(CatalogEntity owned) {
    Entity entity = owner(owned).orElseThrow(() -> new AssertionError("No owner"));
    return entity instanceof UserEntity
        ? ((UserEntity) entity).name()
        : ((GroupEntity) entity).name();
  }

  private NameIdentifier insertPrincipal(boolean group, String name) throws IOException {
    long id = RandomIdGenerator.INSTANCE.nextId();
    if (group) {
      GroupMetaService.getInstance()
          .insertGroup(
              createGroupEntity(
                  id, AuthorizationUtils.ofGroupNamespace(METALAKE), name, AUDIT_INFO, null, null),
              false);
      return AuthorizationUtils.ofGroup(METALAKE, name);
    }
    UserMetaService.getInstance()
        .insertUser(
            createUserEntity(id, AuthorizationUtils.ofUserNamespace(METALAKE), name, AUDIT_INFO),
            false);
    return AuthorizationUtils.ofUser(METALAKE, name);
  }

  private void deletePrincipal(boolean group, NameIdentifier principal) {
    if (group) {
      assertTrue(GroupMetaService.getInstance().deleteGroup(principal));
    } else {
      assertTrue(UserMetaService.getInstance().deleteUser(principal));
    }
  }

  private long principalId(boolean group, NameIdentifier principal) {
    return EntityIdService.getEntityId(principal, type(group));
  }

  private Entity.EntityType type(boolean group) {
    return group ? Entity.EntityType.GROUP : Entity.EntityType.USER;
  }

  private long liveOwnerRows(long ownerId) throws Exception {
    return queryLong(
        "SELECT COUNT(*) FROM owner_meta WHERE owner_id = " + ownerId + " AND deleted_at = 0");
  }

  private long liveOwnerRowsForObject(CatalogEntity owned) throws Exception {
    return queryLong(
        "SELECT COUNT(*) FROM owner_meta WHERE metadata_object_id = "
            + owned.id()
            + " AND metadata_object_type = 'CATALOG' AND deleted_at = 0");
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
    return whileTransactionHeld(holder, contender, () -> {}, true, false);
  }

  private Throwable whileTransactionHeld(
      Executable holder, Executable contender, boolean standaloneContender) throws Exception {
    return whileTransactionHeld(holder, contender, () -> {}, true, standaloneContender);
  }

  private Throwable whileTransactionHeld(
      Executable holder, Executable contender, Executable beforeCompletion, boolean commitHolder)
      throws Exception {
    return whileTransactionHeld(holder, contender, beforeCompletion, commitHolder, false);
  }

  /**
   * Runs {@code holder} inside a transaction held open on this thread, starts {@code contender} on
   * another thread, waits until the database reports the contender blocked on the holder, then
   * commits or rolls back the holder and returns the contender's failure, or null when it
   * committed.
   *
   * <p>By default the contender is wrapped in a transaction of its own so that its session can be
   * identified. A {@code standaloneContender} runs exactly as production does, owning its
   * transactions, which is what the owner assignment needs to replay a lost race: its session is
   * then not known in advance and the wait is recognised by the holder side alone.
   */
  private Throwable whileTransactionHeld(
      Executable holder,
      Executable contender,
      Executable beforeCompletion,
      boolean commitHolder,
      boolean standaloneContender)
      throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CompletableFuture<Long> started = new CompletableFuture<>();
    if (standaloneContender) {
      raiseDefaultLockTimeout();
    }
    SessionUtils.beginTransaction();
    try {
      long holderId = prepareTransaction();
      Assertions.assertDoesNotThrow(holder);
      Future<Throwable> result =
          standaloneContender
              ? executor.submit(
                  () -> {
                    try {
                      contender.execute();
                      return null;
                    } catch (Throwable failure) {
                      return failure;
                    }
                  })
              : submitTransaction(executor, started, contender);
      awaitBlockedBy(
          result, standaloneContender ? null : started.get(10, TimeUnit.SECONDS), holderId);
      Assertions.assertDoesNotThrow(beforeCompletion);
      if (commitHolder) {
        SessionUtils.commitTransaction();
      } else {
        SessionUtils.rollbackTransaction();
      }
      return result.get(10, TimeUnit.SECONDS);
    } finally {
      SessionUtils.rollbackTransaction();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private Future<Throwable> submitTransaction(
      ExecutorService executor, CompletableFuture<Long> started, Executable operation) {
    return executor.submit(
        () -> {
          SessionUtils.beginTransaction();
          try {
            started.complete(prepareTransaction());
            operation.execute();
            SessionUtils.commitTransaction();
            return null;
          } catch (Throwable failure) {
            started.completeExceptionally(failure);
            return failure;
          } finally {
            SessionUtils.rollbackTransaction();
          }
        });
  }

  private long prepareTransaction() throws SQLException {
    SqlSession session = SqlSessions.getSqlSession();
    try (Statement statement = session.getConnection().createStatement()) {
      String sessionIdQuery;
      switch (backendType) {
        case "h2":
          // Keep the engine timeout above the test's lock-observation deadline.
          statement.execute("SET LOCK_TIMEOUT 30000");
          sessionIdQuery = "SELECT SESSION_ID()";
          break;
        case "mysql":
          statement.execute("SET SESSION innodb_lock_wait_timeout = 30");
          sessionIdQuery = "SELECT CONNECTION_ID()";
          break;
        case "postgresql":
          statement.execute("SET LOCAL lock_timeout = '30s'");
          sessionIdQuery = "SELECT pg_backend_pid()";
          break;
        default:
          throw new IllegalStateException("Unsupported backend: " + backendType);
      }
      try (ResultSet rows = statement.executeQuery(sessionIdQuery)) {
        assertTrue(rows.next());
        return rows.getLong(1);
      }
    } finally {
      SqlSessions.closeSqlSession();
    }
  }

  /**
   * H2 gives new sessions a one-second lock timeout. A standalone contender opens its own sessions,
   * so the database-wide default is raised instead of a per-session setting.
   */
  private void raiseDefaultLockTimeout() throws SQLException {
    if (!"h2".equals(backendType)) {
      return;
    }
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Statement statement = session.getConnection().createStatement()) {
      statement.execute("SET DEFAULT_LOCK_TIMEOUT 30000");
    }
  }

  /**
   * Waits until the database reports a session blocked by the holder: the given contender session
   * when known, otherwise any session. On MySQL a waiter on the implicit lock of an uncommitted
   * insert is reported as blocked by itself rather than by the inserting transaction, so that shape
   * is accepted too.
   */
  private void awaitBlockedBy(Future<Throwable> result, @Nullable Long contenderId, long holderId)
      throws Exception {
    String query;
    switch (backendType) {
      case "h2":
        query =
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SESSIONS WHERE BLOCKER_ID = "
                + holderId
                + (contenderId == null ? "" : " AND SESSION_ID = " + contenderId);
        break;
      case "mysql":
        query =
            "SELECT COUNT(*) FROM performance_schema.data_lock_waits w"
                + " JOIN performance_schema.threads r ON r.THREAD_ID = w.REQUESTING_THREAD_ID"
                + " JOIN performance_schema.threads b ON b.THREAD_ID = w.BLOCKING_THREAD_ID"
                + " WHERE (b.PROCESSLIST_ID = "
                + holderId
                + " OR b.THREAD_ID = r.THREAD_ID)"
                + (contenderId == null ? "" : " AND r.PROCESSLIST_ID = " + contenderId);
        break;
      case "postgresql":
        query =
            "SELECT COUNT(*) FROM pg_stat_activity a WHERE "
                + holderId
                + " = ANY(pg_blocking_pids(a.pid))"
                + (contenderId == null ? "" : " AND a.pid = " + contenderId);
        break;
      default:
        throw new IllegalStateException("Unsupported backend: " + backendType);
    }
    // Observe the actual waiter/blocker pair. A slow thread or connection checkout alone cannot
    // satisfy this assertion, and an unexpectedly completed operation fails immediately.
    try (SqlSession observer =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      Connection connection = observer.getConnection();
      await()
          .pollInSameThread()
          .atMost(10, TimeUnit.SECONDS)
          .pollInterval(10, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                if (result.isDone()) {
                  throw new AssertionError(
                      "Operation completed without waiting for the holder", result.get());
                }
                try (Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery(query)) {
                  return rows.next() && rows.getLong(1) > 0;
                }
              });
    }
  }
}
