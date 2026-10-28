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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.RaceTestUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Replays the seven original OCC entity types through the backend, without manager tree locks. Each
 * worker uses its own thread-bound JDBC session, like servers sharing a database.
 *
 * <p>Complements the cascade fences in {@link TestSchemaMetaService}, relation fences in {@link
 * TestOwnerAssignmentWrites}, and statement snapshots in {@link TestConsistentEntityReads}.
 */
@Tag("gravitino-docker-test")
public class TestEntityStoreConcurrency extends TestJDBCBackend {

  private static final String METALAKE = "occ_races";
  private static final String CATALOG = "parent_catalog";
  private static final String SCHEMA = "parent_schema";
  private static final long TIMEOUT_SECONDS = 30;

  @BeforeEach
  void createParents() throws Exception {
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
  }

  @TestTemplate
  void testCatalogLostUpdate() throws Exception {
    assertLostUpdate(fixture(EntityType.CATALOG));
  }

  @TestTemplate
  void testCatalogRenameCollision() throws Exception {
    assertRenameCollision(fixture(EntityType.CATALOG));
  }

  @TestTemplate
  void testSchemaLostUpdate() throws Exception {
    assertLostUpdate(fixture(EntityType.SCHEMA));
  }

  @TestTemplate
  void testSchemaRenameCollision() throws Exception {
    assertRenameCollision(fixture(EntityType.SCHEMA));
  }

  @TestTemplate
  void testTableLostUpdate() throws Exception {
    assertLostUpdate(fixture(EntityType.TABLE));
  }

  @TestTemplate
  void testTableRenameCollision() throws Exception {
    assertRenameCollision(fixture(EntityType.TABLE));
  }

  @TestTemplate
  void testFilesetLostUpdate() throws Exception {
    assertLostUpdate(fixture(EntityType.FILESET));
  }

  @TestTemplate
  void testFilesetRenameCollision() throws Exception {
    assertRenameCollision(fixture(EntityType.FILESET));
  }

  @TestTemplate
  void testTopicLostUpdate() throws Exception {
    assertLostUpdate(fixture(EntityType.TOPIC));
  }

  @TestTemplate
  void testTopicRenameCollision() throws Exception {
    assertRenameCollision(fixture(EntityType.TOPIC));
  }

  @TestTemplate
  void testViewLostUpdate() throws Exception {
    assertLostUpdate(fixture(EntityType.VIEW));
  }

  @TestTemplate
  void testViewRenameCollision() throws Exception {
    assertRenameCollision(fixture(EntityType.VIEW));
  }

  @TestTemplate
  void testFunctionLostUpdate() throws Exception {
    assertLostUpdate(fixture(EntityType.FUNCTION));
  }

  @TestTemplate
  void testFunctionRenameCollision() throws Exception {
    assertRenameCollision(fixture(EntityType.FUNCTION));
  }

  @TestTemplate
  void testConcurrentVersionLinksReserveDistinctNumbers() throws Exception {
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE, CATALOG, SCHEMA),
            "model",
            "model",
            0,
            Map.of(),
            AUDIT_INFO);
    backend.insert(model, false);
    int writers = 4;
    CyclicBarrier observedSameModel = new CyclicBarrier(writers);
    AtomicInteger observed = new AtomicInteger();
    List<Callable<?>> actions = new ArrayList<>();
    for (int i = 0; i < writers; i++) {
      int writer = i;
      actions.add(
          () -> {
            AtomicBoolean firstModelRead = new AtomicBoolean();
            try (MockedStatic<SessionUtils> sessions =
                Mockito.mockStatic(SessionUtils.class, Mockito.CALLS_REAL_METHODS)) {
              sessions
                  .when(
                      () ->
                          SessionUtils.getWithoutCommit(
                              Mockito.eq(ModelMetaMapper.class), Mockito.any()))
                  .thenAnswer(
                      invocation -> {
                        Object result = invocation.callRealMethod();
                        if (result instanceof ModelPO
                            && firstModelRead.compareAndSet(false, true)) {
                          observed.incrementAndGet();
                          await(observedSameModel);
                        }
                        return result;
                      });
              ModelVersionEntity version =
                  ModelVersionEntity.builder()
                      .withModelIdentifier(model.nameIdentifier())
                      .withVersion(0)
                      .withUris(Map.of(ModelVersion.URI_NAME_UNKNOWN, "uri_" + writer))
                      .withAliases(List.of("alias_" + writer))
                      .withComment("writer_" + writer)
                      .withProperties(Map.of())
                      .withAuditInfo(AUDIT_INFO)
                      .build();
              backend.insert(version, false);
              return version;
            }
          });
    }
    List<Object> outcomes = RaceTestUtils.runTogether(actions);
    outcomes.forEach(outcome -> assertInstanceOf(ModelVersionEntity.class, outcome));
    assertEquals(writers, observed.get());
    List<ModelVersionEntity> stored =
        backend.list(
            Namespace.of(METALAKE, CATALOG, SCHEMA, "model"), EntityType.MODEL_VERSION, true);
    assertEquals(writers, stored.size());
    assertEquals(
        Set.of(0, 1, 2, 3),
        stored.stream().map(ModelVersionEntity::version).collect(Collectors.toSet()));
    for (int i = 0; i < writers; i++) {
      String marker = "writer_" + i;
      ModelVersionEntity version =
          stored.stream().filter(v -> marker.equals(v.comment())).findFirst().orElseThrow();
      assertEquals(Map.of(ModelVersion.URI_NAME_UNKNOWN, "uri_" + i), version.uris());
      assertEquals(List.of("alias_" + i), version.aliases());
    }
    assertEquals(
        writers, backend.<ModelEntity>get(model.nameIdentifier(), model.type()).latestVersion());
  }

  @TestTemplate
  void testConcurrentSameIdTableOverwritesKeepOneCompleteSnapshot() throws Exception {
    long tableId = RandomIdGenerator.INSTANCE.nextId();
    TableEntity initial = tablePayload(tableId, "initial");
    TableEntity left = tablePayload(tableId, "left");
    TableEntity right = tablePayload(tableId, "right");
    backend.insert(initial, false);
    List<Object> outcomes =
        RaceTestUtils.runTogether(
            List.of(
                () -> {
                  backend.insert(left, true);
                  return left;
                },
                () -> {
                  backend.insert(right, true);
                  return right;
                }));
    outcomes.forEach(outcome -> assertInstanceOf(TableEntity.class, outcome));
    TableEntity stored = backend.get(initial.nameIdentifier(), initial.type());
    TableEntity expected = Map.of("left", left, "right", right).get(stored.comment());
    assertNotNull(expected);
    assertEquals(tableId, stored.id());
    assertEquals(expected.columns(), stored.columns());
    assertEquals(expected.properties(), stored.properties());
    TablePO metadata =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper ->
                mapper.selectTableByFullQualifiedName(METALAKE, CATALOG, SCHEMA, initial.name()));
    assertEquals(3L, metadata.getCurrentVersion());
  }

  @TestTemplate
  void testDeletedUserCannotGainRoleFromStaleUpdate() throws Exception {
    RoleEntity role = role();
    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE),
            "user",
            AUDIT_INFO,
            List.of(),
            List.of());
    assertDropRejectsRelationUpdate(
        user,
        current ->
            UserEntity.builder()
                .withId(current.id())
                .withName(current.name())
                .withNamespace(current.namespace())
                .withAuditInfo(current.auditInfo())
                .withRoleNames(List.of(role.name()))
                .withRoleIds(List.of(role.id()))
                .build());
    assertTrue(backend.exists(role.nameIdentifier(), role.type()));
  }

  @TestTemplate
  void testDeletedGroupCannotGainRoleFromStaleUpdate() throws Exception {
    RoleEntity role = role();
    GroupEntity group =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(METALAKE),
            "group",
            AUDIT_INFO,
            List.of(),
            List.of());
    assertDropRejectsRelationUpdate(
        group,
        current ->
            GroupEntity.builder()
                .withId(current.id())
                .withName(current.name())
                .withNamespace(current.namespace())
                .withAuditInfo(current.auditInfo())
                .withRoleNames(List.of(role.name()))
                .withRoleIds(List.of(role.id()))
                .build());
    assertTrue(backend.exists(role.nameIdentifier(), role.type()));
  }

  private RoleEntity role() throws Exception {
    RoleEntity role =
        RoleEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("role")
            .withNamespace(AuthorizationUtils.ofRoleNamespace(METALAKE))
            .withAuditInfo(AUDIT_INFO)
            .withProperties(Map.of())
            .withSecurableObjects(List.of())
            .build();
    backend.insert(role, false);
    return role;
  }

  private <E extends Entity & HasIdentifier> void assertDropRejectsRelationUpdate(
      E initial, Function<E, E> update) throws Exception {
    backend.insert(initial, false);
    CountDownLatch staleRead = new CountDownLatch(1);
    CountDownLatch deleted = new CountDownLatch(1);
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      Future<Throwable> updateResult =
          worker.submit(
              () -> {
                try {
                  backend.<E>update(
                      initial.nameIdentifier(),
                      initial.type(),
                      current -> {
                        staleRead.countDown();
                        await(deleted);
                        return update.apply(current);
                      });
                  return null;
                } catch (Throwable failure) {
                  staleRead.countDown();
                  return failure;
                }
              });
      await(staleRead);
      assertTrue(backend.delete(initial.nameIdentifier(), initial.type(), false));
      deleted.countDown();
      Throwable failure = updateResult.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (!(failure instanceof NoSuchEntityException)
          && !(failure instanceof OptimisticLockException)) {
        throw new AssertionError(
            "A stale membership update must fail after its principal is dropped", failure);
      }
      assertFalse(backend.exists(initial.nameIdentifier(), initial.type()));
      String relation = initial.type() == EntityType.USER ? "user_role_rel" : "group_role_rel";
      String principalId = initial.type() == EntityType.USER ? "user_id" : "group_id";
      try (SqlSession session =
              SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
          PreparedStatement statement =
              session
                  .getConnection()
                  .prepareStatement(
                      "SELECT COUNT(*) FROM "
                          + relation
                          + " WHERE "
                          + principalId
                          + " = ? AND deleted_at = 0")) {
        statement.setLong(1, initial.id());
        try (ResultSet rows = statement.executeQuery()) {
          assertTrue(rows.next());
          assertEquals(
              0, rows.getInt(1), "A dropped principal must not retain an active membership");
        }
      }
    } finally {
      deleted.countDown();
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
  }

  private <E extends Entity & HasIdentifier> void assertLostUpdate(EntityCase<E> testCase)
      throws Exception {
    E initial = testCase.factory.create(RandomIdGenerator.INSTANCE.nextId(), "entity", "0");
    backend.insert(initial, false);
    CountDownLatch staleRead = new CountDownLatch(1);
    CountDownLatch winnerCommitted = new CountDownLatch(1);
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      Future<Throwable> staleWriter =
          worker.submit(
              () -> {
                try {
                  backend.<E>update(
                      initial.nameIdentifier(),
                      initial.type(),
                      current -> {
                        assertEquals("0", testCase.comment.apply(current));
                        staleRead.countDown();
                        await(winnerCommitted);
                        return testCase.factory.create(current.id(), current.name(), "1");
                      });
                  return null;
                } catch (Throwable failure) {
                  staleRead.countDown();
                  return failure;
                }
              });
      await(staleRead);
      backend.<E>update(
          initial.nameIdentifier(),
          initial.type(),
          current -> testCase.factory.create(current.id(), current.name(), "1"));
      winnerCommitted.countDown();
      Throwable failure = staleWriter.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (!(failure instanceof OptimisticLockException)) {
        throw new AssertionError("The stale writer must fail its version check", failure);
      }
      assertEquals(
          "1", testCase.comment.apply(backend.get(initial.nameIdentifier(), initial.type())));
      // Retrying the rejected read-modify-write must use the committed state, not replay "1".
      backend.<E>update(
          initial.nameIdentifier(),
          initial.type(),
          current ->
              testCase.factory.create(
                  current.id(),
                  current.name(),
                  Integer.toString(Integer.parseInt(testCase.comment.apply(current)) + 1)));
      assertEquals(
          "2", testCase.comment.apply(backend.get(initial.nameIdentifier(), initial.type())));
    } finally {
      winnerCommitted.countDown();
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
  }

  private <E extends Entity & HasIdentifier> void assertRenameCollision(EntityCase<E> testCase)
      throws Exception {
    E leftInput = testCase.factory.create(RandomIdGenerator.INSTANCE.nextId(), "left", "left");
    E rightInput = testCase.factory.create(RandomIdGenerator.INSTANCE.nextId(), "right", "right");
    E created = testCase.factory.create(RandomIdGenerator.INSTANCE.nextId(), "target", "created");
    backend.insert(leftInput, false);
    backend.insert(rightInput, false);
    E left = backend.get(leftInput.nameIdentifier(), leftInput.type());
    E right = backend.get(rightInput.nameIdentifier(), rightInput.type());
    CyclicBarrier bothRenamesObserved = new CyclicBarrier(3);
    List<Object> outcomes =
        RaceTestUtils.runTogether(
            List.of(
                () ->
                    backend.<E>update(
                        left.nameIdentifier(),
                        left.type(),
                        current -> {
                          await(bothRenamesObserved);
                          return testCase.factory.create(current.id(), "target", "left");
                        }),
                () ->
                    backend.<E>update(
                        right.nameIdentifier(),
                        right.type(),
                        current -> {
                          await(bothRenamesObserved);
                          return testCase.factory.create(current.id(), "target", "right");
                        }),
                () -> {
                  await(bothRenamesObserved);
                  backend.insert(created, false);
                  return created;
                }));
    assertEquals(1, outcomes.stream().filter(outcome -> !(outcome instanceof Throwable)).count());
    for (Object outcome : outcomes) {
      if (outcome instanceof Throwable) {
        if (!expectedNameConflict((Throwable) outcome)) {
          throw new AssertionError("Unexpected naming-race failure", (Throwable) outcome);
        }
      }
    }
    E target = backend.get(created.nameIdentifier(), created.type());
    Object winner =
        outcomes.stream()
            .filter(outcome -> !(outcome instanceof Throwable))
            .findFirst()
            .orElseThrow();
    assertEquals(((HasIdentifier) winner).id(), target.id());
    String winnerMarker =
        left.id().equals(target.id())
            ? "left"
            : right.id().equals(target.id()) ? "right" : "created";
    assertEquals(winnerMarker, testCase.comment.apply(target));
    for (E original : List.of(left, right)) {
      if (original.id().equals(target.id())) {
        assertThrows(
            NoSuchEntityException.class,
            () -> backend.get(original.nameIdentifier(), original.type()));
      } else {
        assertEquals(original, backend.<E>get(original.nameIdentifier(), original.type()));
      }
    }
    List<E> siblings = backend.list(created.namespace(), created.type(), true);
    assertEquals(1, siblings.stream().filter(entity -> entity.name().equals("target")).count());
  }

  private boolean expectedNameConflict(Throwable failure) {
    if (failure instanceof EntityAlreadyExistsException
        || failure instanceof OptimisticLockException) {
      return true;
    }
    // A database may choose a deadlock/serialization victim while two renames and a create
    // compete for one unique key. Other SQL failures are not an acceptable race outcome.
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException) {
        String state = ((SQLException) cause).getSQLState();
        if ("40001".equals(state) || "40P01".equals(state)) {
          return true;
        }
      }
    }
    return false;
  }

  private EntityCase<?> fixture(EntityType type) {
    Namespace namespace = Namespace.of(METALAKE, CATALOG, SCHEMA);
    switch (type) {
      case CATALOG:
        return new EntityCase<>(
            (id, name, comment) ->
                CatalogEntity.builder()
                    .withId(id)
                    .withName(name)
                    .withNamespace(Namespace.of(METALAKE))
                    .withType(Catalog.Type.RELATIONAL)
                    .withProvider("test")
                    .withComment(comment)
                    .withProperties(Map.of())
                    .withAuditInfo(AUDIT_INFO)
                    .build(),
            CatalogEntity::getComment);
      case SCHEMA:
        return new EntityCase<>(
            (id, name, comment) ->
                SchemaEntity.builder()
                    .withId(id)
                    .withName(name)
                    .withNamespace(Namespace.of(METALAKE, CATALOG))
                    .withComment(comment)
                    .withProperties(Map.of())
                    .withAuditInfo(AUDIT_INFO)
                    .build(),
            SchemaEntity::comment);
      case TABLE:
        return new EntityCase<>(
            (id, name, comment) ->
                TableEntity.builder()
                    .withId(id)
                    .withName(name)
                    .withNamespace(namespace)
                    .withComment(comment)
                    .withColumns(List.of())
                    .withProperties(Map.of())
                    .withAuditInfo(AUDIT_INFO)
                    .build(),
            TableEntity::comment);
      case FILESET:
        return new EntityCase<>(
            (id, name, comment) ->
                FilesetEntity.builder()
                    .withId(id)
                    .withName(name)
                    .withNamespace(namespace)
                    .withComment(comment)
                    .withFilesetType(Fileset.Type.MANAGED)
                    .withStorageLocations(Map.of(Fileset.LOCATION_NAME_UNKNOWN, "/tmp"))
                    .withProperties(Map.of())
                    .withAuditInfo(AUDIT_INFO)
                    .build(),
            FilesetEntity::comment);
      case TOPIC:
        return new EntityCase<>(
            (id, name, comment) ->
                TopicEntity.builder()
                    .withId(id)
                    .withName(name)
                    .withNamespace(namespace)
                    .withComment(comment)
                    .withProperties(Map.of())
                    .withAuditInfo(AUDIT_INFO)
                    .build(),
            TopicEntity::comment);
      case VIEW:
        ViewEntity shape =
            createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "shape");
        return new EntityCase<>(
            (id, name, comment) ->
                ViewEntity.builder()
                    .withId(id)
                    .withName(name)
                    .withNamespace(namespace)
                    .withComment(comment)
                    .withColumns(shape.columns())
                    .withRepresentations(shape.representations())
                    .withProperties(Map.of())
                    .withAuditInfo(AUDIT_INFO)
                    .build(),
            ViewEntity::comment);
      case FUNCTION:
        FunctionEntity function =
            createFunctionEntity(
                RandomIdGenerator.INSTANCE.nextId(), namespace, "shape", AUDIT_INFO);
        return new EntityCase<>(
            (id, name, comment) ->
                FunctionEntity.builder()
                    .withId(id)
                    .withName(name)
                    .withNamespace(namespace)
                    .withComment(comment)
                    .withFunctionType(function.functionType())
                    .withDeterministic(function.deterministic())
                    .withDefinitions(function.definitions())
                    .withAuditInfo(AUDIT_INFO)
                    .build(),
            FunctionEntity::comment);
      default:
        throw new IllegalArgumentException("Not an original OCC stress-test type: " + type);
    }
  }

  private TableEntity tablePayload(long id, String marker) {
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(marker + "_column")
            .withPosition(0)
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withAuditInfo(AUDIT_INFO)
            .build();
    return TableEntity.builder()
        .withId(id)
        .withName("overwritten_table")
        .withNamespace(Namespace.of(METALAKE, CATALOG, SCHEMA))
        .withComment(marker)
        .withColumns(List.of(column))
        .withProperties(Map.of("generation", marker))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static void await(CountDownLatch signal) {
    try {
      assertTrue(
          signal.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "Worker did not reach the expected boundary");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while coordinating concurrent writers", e);
    }
  }

  private static void await(CyclicBarrier barrier) {
    try {
      barrier.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while coordinating concurrent writers", e);
    } catch (Exception e) {
      throw new IllegalStateException("Writers did not reach the same observed state", e);
    }
  }

  @FunctionalInterface
  private interface EntityFactory<E extends Entity & HasIdentifier> {
    E create(long id, String name, String comment);
  }

  private static final class EntityCase<E extends Entity & HasIdentifier> {
    private final EntityFactory<E> factory;
    private final Function<E, String> comment;

    private EntityCase(EntityFactory<E> factory, Function<E, String> comment) {
      this.factory = factory;
      this.comment = comment;
    }
  }
}
