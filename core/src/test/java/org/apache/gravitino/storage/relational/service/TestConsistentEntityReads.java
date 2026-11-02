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
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Exercises aggregate reads across a real concurrent commit on every JDBC backend. */
public class TestConsistentEntityReads extends TestJDBCBackend {
  private static final String METALAKE = "consistent_reads";
  private static final String CATALOG = "catalog";
  private static final String SCHEMA = "schema";

  @TestTemplate
  void testTableOverwritePreservesSnapshot() throws Exception {
    checkTableOverwrite(false, false);
  }

  @TestTemplate
  void testTableReplacementPreservesSnapshot() throws Exception {
    checkTableOverwrite(true, false);
  }

  @TestTemplate
  void testCachedTableReadNeverPublishesEmptyColumns() throws Exception {
    checkTableOverwrite(false, true);
  }

  @TestTemplate
  void testTableDropPreservesSnapshot() throws Exception {
    parents();
    TableEntity original = table(RandomIdGenerator.INSTANCE.nextId(), "before", "old_column");
    backend.insert(original, false);
    TableEntity read =
        afterRootRead(
            TableMetaMapper.class,
            () -> backend.get(original.nameIdentifier(), Entity.EntityType.TABLE),
            () -> backend.delete(original.nameIdentifier(), Entity.EntityType.TABLE, true));
    assertTableSnapshot(original, read);
    assertThrows(
        NoSuchEntityException.class,
        () -> backend.get(original.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @TestTemplate
  void testEmptyTableIsNotMistakenForMissing() throws Exception {
    parents();
    TableEntity empty =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("empty")
            .withNamespace(Namespace.of(METALAKE, CATALOG, SCHEMA))
            .withColumns(List.of())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(empty, false);
    TableEntity read = backend.get(empty.nameIdentifier(), Entity.EntityType.TABLE);
    assertEquals(empty.id(), read.id());
    assertTrue(read.columns().isEmpty());
  }

  @TestTemplate
  void testRoleOverwritePreservesPrivilegeSnapshot() throws Exception {
    parents();
    createAndInsertCatalog(METALAKE, "other_catalog");
    RoleEntity before = role("before", CATALOG);
    RoleEntity after = role("after", "other_catalog");
    backend.insert(before, false);
    RoleEntity read =
        afterRootRead(
            RoleMetaMapper.class,
            () -> backend.get(before.nameIdentifier(), Entity.EntityType.ROLE),
            () -> backend.insert(after, true));
    assertEquals(before.properties(), read.properties());
    assertEquals(1, read.securableObjects().size());
    assertEquals(CATALOG, read.securableObjects().get(0).fullName());
    RoleEntity current = backend.get(before.nameIdentifier(), Entity.EntityType.ROLE);
    assertEquals(after.properties(), current.properties());
    assertEquals("other_catalog", current.securableObjects().get(0).fullName());
  }

  @TestTemplate
  void testRoleDropPreservesPrivilegeSnapshot() throws Exception {
    parents();
    RoleEntity before = role("before", CATALOG);
    backend.insert(before, false);
    RoleEntity read =
        afterRootRead(
            RoleMetaMapper.class,
            () -> backend.get(before.nameIdentifier(), Entity.EntityType.ROLE),
            () -> backend.delete(before.nameIdentifier(), Entity.EntityType.ROLE, true));
    assertEquals(1, read.securableObjects().size());
    assertEquals(CATALOG, read.securableObjects().get(0).fullName());
    assertThrows(
        NoSuchEntityException.class,
        () -> backend.get(before.nameIdentifier(), Entity.EntityType.ROLE));
  }

  @TestTemplate
  void testRoleListingPreservesPrivilegeSnapshot() throws Exception {
    parents();
    createAndInsertCatalog(METALAKE, "other_catalog");
    RoleEntity before = role("before", CATALOG);
    RoleEntity after = role("after", "other_catalog");
    backend.insert(before, false);
    List<RoleEntity> read =
        afterRootRead(
            RoleMetaMapper.class,
            () ->
                RoleMetaService.getInstance()
                    .listRolesByMetadataObject(
                        NameIdentifier.of(METALAKE, CATALOG), Entity.EntityType.CATALOG, true),
            () -> backend.insert(after, true));
    assertEquals(1, read.size());
    assertEquals(before.properties(), read.get(0).properties());
    assertEquals(CATALOG, read.get(0).securableObjects().get(0).fullName());
  }

  @TestTemplate
  void testUserOverwritePreservesMembershipSnapshot() throws Exception {
    parents();
    RoleEntity first = role("before", CATALOG);
    RoleEntity second =
        createRoleEntity(12346L, first.namespace(), "second_role", AUDIT_INFO, CATALOG);
    backend.insert(first, false);
    backend.insert(second, false);
    UserEntity before =
        createUserEntity(
            12347L,
            AuthorizationUtils.ofUserNamespace(METALAKE),
            "principal",
            AUDIT_INFO,
            List.of(first.name()),
            List.of(first.id()));
    AuditInfo afterAudit =
        AuditInfo.builder().withCreator("after").withCreateTime(AUDIT_INFO.createTime()).build();
    UserEntity after =
        createUserEntity(
            before.id(),
            before.namespace(),
            before.name(),
            afterAudit,
            List.of(second.name()),
            List.of(second.id()));
    backend.insert(before, false);
    UserEntity read =
        afterRootRead(
            UserMetaMapper.class,
            () -> backend.get(before.nameIdentifier(), Entity.EntityType.USER),
            () -> backend.insert(after, true));
    assertEquals(before.auditInfo(), read.auditInfo());
    assertEquals(before.roleNames(), read.roleNames());
    assertEquals(before.roleIds(), read.roleIds());
    UserEntity current = backend.get(before.nameIdentifier(), Entity.EntityType.USER);
    assertEquals(after.auditInfo(), current.auditInfo());
    assertEquals(after.roleIds(), current.roleIds());
  }

  @TestTemplate
  void testUserDropPreservesMembershipSnapshot() throws Exception {
    parents();
    RoleEntity role = role("before", CATALOG);
    backend.insert(role, false);
    UserEntity before =
        createUserEntity(
            12347L,
            AuthorizationUtils.ofUserNamespace(METALAKE),
            "principal",
            AUDIT_INFO,
            List.of(role.name()),
            List.of(role.id()));
    backend.insert(before, false);
    UserEntity read =
        afterRootRead(
            UserMetaMapper.class,
            () -> backend.get(before.nameIdentifier(), Entity.EntityType.USER),
            () -> backend.delete(before.nameIdentifier(), Entity.EntityType.USER, true));
    assertEquals(before.roleNames(), read.roleNames());
    assertEquals(before.roleIds(), read.roleIds());
    assertThrows(
        NoSuchEntityException.class,
        () -> backend.get(before.nameIdentifier(), Entity.EntityType.USER));
  }

  @TestTemplate
  void testGroupOverwritePreservesMembershipSnapshot() throws Exception {
    parents();
    RoleEntity first = role("before", CATALOG);
    RoleEntity second =
        createRoleEntity(12346L, first.namespace(), "second_role", AUDIT_INFO, CATALOG);
    backend.insert(first, false);
    backend.insert(second, false);
    GroupEntity before =
        createGroupEntity(
            12347L,
            AuthorizationUtils.ofGroupNamespace(METALAKE),
            "principal",
            AUDIT_INFO,
            List.of(first.name()),
            List.of(first.id()));
    AuditInfo afterAudit =
        AuditInfo.builder().withCreator("after").withCreateTime(AUDIT_INFO.createTime()).build();
    GroupEntity after =
        createGroupEntity(
            before.id(),
            before.namespace(),
            before.name(),
            afterAudit,
            List.of(second.name()),
            List.of(second.id()));
    backend.insert(before, false);
    GroupEntity read =
        afterRootRead(
            GroupMetaMapper.class,
            () -> backend.get(before.nameIdentifier(), Entity.EntityType.GROUP),
            () -> backend.insert(after, true));
    assertEquals(before.auditInfo(), read.auditInfo());
    assertEquals(before.roleNames(), read.roleNames());
    assertEquals(before.roleIds(), read.roleIds());
    GroupEntity current = backend.get(before.nameIdentifier(), Entity.EntityType.GROUP);
    assertEquals(after.auditInfo(), current.auditInfo());
    assertEquals(after.roleIds(), current.roleIds());
  }

  @TestTemplate
  void testGroupDropPreservesMembershipSnapshot() throws Exception {
    parents();
    RoleEntity role = role("before", CATALOG);
    backend.insert(role, false);
    GroupEntity before =
        createGroupEntity(
            12347L,
            AuthorizationUtils.ofGroupNamespace(METALAKE),
            "principal",
            AUDIT_INFO,
            List.of(role.name()),
            List.of(role.id()));
    backend.insert(before, false);
    GroupEntity read =
        afterRootRead(
            GroupMetaMapper.class,
            () -> backend.get(before.nameIdentifier(), Entity.EntityType.GROUP),
            () -> backend.delete(before.nameIdentifier(), Entity.EntityType.GROUP, true));
    assertEquals(before.roleNames(), read.roleNames());
    assertEquals(before.roleIds(), read.roleIds());
    assertThrows(
        NoSuchEntityException.class,
        () -> backend.get(before.nameIdentifier(), Entity.EntityType.GROUP));
  }

  @TestTemplate
  void testFullNameTableReadPreservesSnapshot() throws Exception {
    GravitinoEnv environment = Mockito.spy(GravitinoEnv.getInstance());
    Mockito.doReturn(false).when(environment).cacheEnabled();
    try (MockedStatic<GravitinoEnv> environments = Mockito.mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(environment);
      checkTableOverwrite(false, false);
    }
  }

  @TestTemplate
  void testFullNameReadRetainsMissingEntityErrors() throws Exception {
    parents();
    GravitinoEnv environment = Mockito.spy(GravitinoEnv.getInstance());
    Mockito.doReturn(false).when(environment).cacheEnabled();
    try (MockedStatic<GravitinoEnv> environments = Mockito.mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(environment);
      for (NameIdentifier missing :
          List.of(
              NameIdentifier.of(METALAKE, CATALOG, SCHEMA, "absent"),
              NameIdentifier.of(METALAKE, CATALOG, "absent_schema", "absent"),
              NameIdentifier.of(METALAKE, "absent_catalog", SCHEMA, "absent"))) {
        assertThrows(
            NoSuchEntityException.class, () -> backend.get(missing, Entity.EntityType.TABLE));
      }
    }
  }

  @TestTemplate
  void testRolePreservesDistinctRowsForSameTarget() throws Exception {
    parents();
    List<SecurableObject> objects =
        List.of(
            SecurableObjects.ofCatalog(CATALOG, List.of(Privileges.UseCatalog.allow())),
            SecurableObjects.ofCatalog(CATALOG, List.of(Privileges.CreateSchema.allow())));
    RoleEntity role =
        createRoleEntity(
            12345L,
            AuthorizationUtils.ofRoleNamespace(METALAKE),
            "duplicate_targets",
            AUDIT_INFO,
            objects,
            Map.of());
    backend.insert(role, false);
    RoleEntity read = backend.get(role.nameIdentifier(), Entity.EntityType.ROLE);
    assertEquals(2, read.securableObjects().size());
    assertTrue(read.securableObjects().containsAll(objects));
    List<RoleEntity> roles =
        RoleMetaService.getInstance()
            .listRolesByMetadataObject(
                NameIdentifier.of(METALAKE, CATALOG), Entity.EntityType.CATALOG, true);
    assertEquals(1, roles.size());
    assertEquals(2, roles.get(0).securableObjects().size());
    assertTrue(roles.get(0).securableObjects().containsAll(objects));
    backend.update(
        role.nameIdentifier(),
        Entity.EntityType.ROLE,
        current -> {
          RoleEntity snapshot = (RoleEntity) current;
          assertEquals(2, snapshot.securableObjects().size());
          assertTrue(snapshot.securableObjects().containsAll(objects));
          return snapshot;
        });
  }

  @TestTemplate
  void testEmptyRoleRetainsEmptyPrivilegeSnapshot() throws Exception {
    parents();
    RoleEntity replacement = role("after", CATALOG);
    RoleEntity empty =
        RoleEntity.builder()
            .withId(replacement.id())
            .withName(replacement.name())
            .withNamespace(replacement.namespace())
            .withProperties(Map.of("marker", "before"))
            .withSecurableObjects(List.of())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(empty, false);
    RoleEntity read =
        afterRootRead(
            RoleMetaMapper.class,
            () -> backend.get(empty.nameIdentifier(), Entity.EntityType.ROLE),
            () -> backend.insert(replacement, true));
    assertEquals(empty.properties(), read.properties());
    assertTrue(read.securableObjects().isEmpty());
    RoleEntity current = backend.get(empty.nameIdentifier(), Entity.EntityType.ROLE);
    assertEquals(replacement.securableObjects(), current.securableObjects());
  }

  @TestTemplate
  void testTableUpdaterReceivesConsistentSnapshot() throws Exception {
    parents();
    TableEntity before = table(RandomIdGenerator.INSTANCE.nextId(), "before", "old_column");
    TableEntity after = table(before.id(), "after", "new_column");
    assertUpdaterSnapshot(TableMetaMapper.class, before, after, this::assertTableSnapshot);
  }

  @TestTemplate
  void testRoleUpdaterReceivesConsistentSnapshot() throws Exception {
    parents();
    createAndInsertCatalog(METALAKE, "other_catalog");
    assertUpdaterSnapshot(
        RoleMetaMapper.class,
        role("before", CATALOG),
        role("after", "other_catalog"),
        (expected, actual) -> {
          assertEquals(expected.properties(), actual.properties());
          assertEquals(expected.securableObjects(), actual.securableObjects());
        });
  }

  @TestTemplate
  void testUserUpdaterReceivesConsistentSnapshot() throws Exception {
    parents();
    RoleEntity first = role("before", CATALOG);
    RoleEntity second =
        createRoleEntity(12346L, first.namespace(), "second_role", AUDIT_INFO, CATALOG);
    backend.insert(first, false);
    backend.insert(second, false);
    Namespace namespace = AuthorizationUtils.ofUserNamespace(METALAKE);
    UserEntity before =
        createUserEntity(
            12347L, namespace, "principal", AUDIT_INFO, List.of(first.name()), List.of(first.id()));
    UserEntity after =
        createUserEntity(
            before.id(),
            namespace,
            before.name(),
            AUDIT_INFO,
            List.of(second.name()),
            List.of(second.id()));
    assertUpdaterSnapshot(
        UserMetaMapper.class,
        before,
        after,
        (expected, actual) -> {
          assertEquals(expected.auditInfo(), actual.auditInfo());
          assertEquals(expected.roleNames(), actual.roleNames());
          assertEquals(expected.roleIds(), actual.roleIds());
        });
  }

  @TestTemplate
  void testGroupUpdaterReceivesConsistentSnapshot() throws Exception {
    parents();
    RoleEntity first = role("before", CATALOG);
    RoleEntity second =
        createRoleEntity(12346L, first.namespace(), "second_role", AUDIT_INFO, CATALOG);
    backend.insert(first, false);
    backend.insert(second, false);
    Namespace namespace = AuthorizationUtils.ofGroupNamespace(METALAKE);
    GroupEntity before =
        createGroupEntity(
            12347L, namespace, "principal", AUDIT_INFO, List.of(first.name()), List.of(first.id()));
    GroupEntity after =
        createGroupEntity(
            before.id(),
            namespace,
            before.name(),
            AUDIT_INFO,
            List.of(second.name()),
            List.of(second.id()));
    assertUpdaterSnapshot(
        GroupMetaMapper.class,
        before,
        after,
        (expected, actual) -> {
          assertEquals(expected.auditInfo(), actual.auditInfo());
          assertEquals(expected.roleNames(), actual.roleNames());
          assertEquals(expected.roleIds(), actual.roleIds());
        });
  }

  @TestTemplate
  void testMetadataOnlyReadsDoNotMasqueradeAsEmptySnapshots() throws Exception {
    parents();
    TableEntity table = table(RandomIdGenerator.INSTANCE.nextId(), "before", "column");
    RoleEntity role = role("before", CATALOG);
    UserEntity user =
        createUserEntity(
            12347L,
            AuthorizationUtils.ofUserNamespace(METALAKE),
            "user",
            AUDIT_INFO,
            List.of(),
            List.of());
    GroupEntity group =
        createGroupEntity(
            12348L,
            AuthorizationUtils.ofGroupNamespace(METALAKE),
            "group",
            AUDIT_INFO,
            List.of(),
            List.of());
    backend.insert(table, false);
    backend.insert(role, false);
    backend.insert(user, false);
    backend.insert(group, false);
    long metalakeId =
        EntityIdService.getEntityId(NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE);
    long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    TablePO tablePO =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.selectTableMetaBySchemaIdAndName(schemaId, table.name()));
    RolePO rolePO =
        SessionUtils.getWithoutCommit(
            RoleMetaMapper.class,
            mapper -> mapper.selectRoleMetaByMetalakeIdAndName(metalakeId, role.name()));
    UserPO userPO =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            mapper -> mapper.selectUserMetaByMetalakeIdAndName(metalakeId, user.name()));
    GroupPO groupPO =
        SessionUtils.getWithoutCommit(
            GroupMetaMapper.class,
            mapper -> mapper.selectGroupMetaByMetalakeIdAndName(metalakeId, group.name()));
    assertNull(tablePO.getColumnPOs());
    assertNull(rolePO.getSecurableObjectPOs());
    assertNull(userPO.getRolePOs());
    assertNull(groupPO.getRolePOs());
    assertThrows(IllegalStateException.class, tablePO::requireColumnPOs);
    assertThrows(IllegalStateException.class, rolePO::requireSecurableObjectPOs);
    assertThrows(IllegalStateException.class, userPO::requireRolePOs);
    assertThrows(IllegalStateException.class, groupPO::requireRolePOs);
    assertMetadataOnlySnapshotRejected(TableMetaMapper.class, tablePO, table);
    assertMetadataOnlySnapshotRejected(RoleMetaMapper.class, rolePO, role);
    assertMetadataOnlySnapshotRejected(UserMetaMapper.class, userPO, user);
    assertMetadataOnlySnapshotRejected(GroupMetaMapper.class, groupPO, group);
    // A real aggregate mapper must initialize an empty collection even without a joined child.
    UserPO loadedUser =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            mapper -> mapper.selectUserWithRolesByMetalakeIdAndName(metalakeId, user.name()));
    GroupPO loadedGroup =
        SessionUtils.getWithoutCommit(
            GroupMetaMapper.class,
            mapper -> mapper.selectGroupWithRolesByMetalakeIdAndName(metalakeId, group.name()));
    assertTrue(loadedUser.requireRolePOs().isEmpty());
    assertTrue(loadedGroup.requireRolePOs().isEmpty());
  }

  private <E extends Entity & HasIdentifier> void assertMetadataOnlySnapshotRejected(
      Class<?> mapper, Object metadataOnly, E entity) throws Exception {
    AtomicBoolean updaterInvoked = new AtomicBoolean();
    try (MockedStatic<SessionUtils> ignored =
        Mockito.mockStatic(
            SessionUtils.class,
            invocation -> {
              Object result = invocation.callRealMethod();
              if (invocation.getMethod().getName().equals("getWithoutCommit")
                  && invocation.getArgument(0) == mapper
                  && result != null
                  && result.getClass() == metadataOnly.getClass()) {
                return metadataOnly;
              }
              return result;
            })) {
      assertThrows(
          IllegalStateException.class, () -> backend.get(entity.nameIdentifier(), entity.type()));
      assertThrows(
          IllegalStateException.class,
          () ->
              backend.<E>update(
                  entity.nameIdentifier(),
                  entity.type(),
                  current -> {
                    updaterInvoked.set(true);
                    return current;
                  }));
      assertFalse(updaterInvoked.get(), "metadata-only input must be rejected before the updater");
    }
  }

  private <E extends Entity & HasIdentifier> void assertUpdaterSnapshot(
      Class<?> mapper, E before, E after, BiConsumer<E, E> assertSnapshot) throws Exception {
    backend.insert(before, false);
    AtomicBoolean invoked = new AtomicBoolean();
    afterRootRead(
        mapper,
        () ->
            assertThrows(
                OptimisticLockException.class,
                () ->
                    backend.<E>update(
                        before.nameIdentifier(),
                        before.type(),
                        current -> {
                          assertSnapshot.accept(before, current);
                          invoked.set(true);
                          return current;
                        })),
        () -> backend.insert(after, true));
    assertTrue(invoked.get(), "the updater must receive the observed snapshot before CAS fails");
    assertSnapshot.accept(after, backend.get(after.nameIdentifier(), after.type()));
  }

  private void checkTableOverwrite(boolean replaceId, boolean cached) throws Exception {
    parents();
    TableEntity original = table(RandomIdGenerator.INSTANCE.nextId(), "before", "old_column");
    TableEntity replacement =
        table(
            replaceId ? RandomIdGenerator.INSTANCE.nextId() : original.id(), "after", "new_column");
    backend.insert(original, false);
    RelationalEntityStore store = new RelationalEntityStore();
    CaffeineEntityCache cache = new CaffeineEntityCache(new Config(false) {});
    try {
      FieldUtils.writeField(store, "backend", backend, true);
      FieldUtils.writeField(store, "cache", cache, true);
      TableEntity read;
      read =
          afterRootRead(
              TableMetaMapper.class,
              () -> {
                try {
                  return cached
                      ? store.get(
                          original.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class)
                      : backend.get(original.nameIdentifier(), Entity.EntityType.TABLE);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              },
              () -> backend.insert(replacement, true));
      assertTableSnapshot(original, read);
      if (cached) {
        assertTableSnapshot(
            original,
            store.get(original.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class));
      }
      TableEntity current = backend.get(original.nameIdentifier(), Entity.EntityType.TABLE);
      assertTableSnapshot(replacement, current);
    } finally {
      cache.clear();
    }
  }

  private void parents() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, CATALOG);
    createAndInsertSchema(METALAKE, CATALOG, SCHEMA);
  }

  private TableEntity table(long id, String comment, String columnName) {
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(columnName)
            .withPosition(0)
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withAuditInfo(AUDIT_INFO)
            .build();
    return TableEntity.builder()
        .withId(id)
        .withName("table")
        .withNamespace(Namespace.of(METALAKE, CATALOG, SCHEMA))
        .withColumns(
            List.of(
                column,
                ColumnEntity.builder()
                    .withId(RandomIdGenerator.INSTANCE.nextId())
                    .withName(columnName + "_second")
                    .withPosition(1)
                    .withDataType(Types.StringType.get())
                    .withNullable(false)
                    .withAutoIncrement(false)
                    .withAuditInfo(AUDIT_INFO)
                    .build()))
        .withComment(comment)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private RoleEntity role(String marker, String catalog) {
    RoleEntity role =
        createRoleEntity(
            12345L, AuthorizationUtils.ofRoleNamespace(METALAKE), "role", AUDIT_INFO, catalog);
    return RoleEntity.builder()
        .withId(role.id())
        .withName(role.name())
        .withNamespace(role.namespace())
        .withProperties(Map.of("marker", marker))
        .withSecurableObjects(role.securableObjects())
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private void assertTableSnapshot(TableEntity expected, TableEntity actual) {
    assertEquals(expected.id(), actual.id());
    assertEquals(expected.comment(), actual.comment());
    assertEquals(expected.columns().size(), actual.columns().size());
    Map<Long, ColumnEntity> actualById = new HashMap<>();
    actual.columns().forEach(column -> actualById.put(column.id(), column));
    assertEquals(expected.columns().size(), actualById.size());
    for (ColumnEntity column : expected.columns()) {
      ColumnEntity observed = actualById.get(column.id());
      assertEquals(column.name(), observed.name());
      assertEquals(column.position(), observed.position());
      assertEquals(column.dataType(), observed.dataType());
      assertEquals(column.nullable(), observed.nullable());
    }
  }

  private <T> T afterRootRead(Class<?> rootMapper, CheckedRead<T> read, CheckedWrite write)
      throws Exception {
    ExecutorService writer = Executors.newSingleThreadExecutor();
    AtomicBoolean committed = new AtomicBoolean();
    try (MockedStatic<SessionUtils> ignored =
        Mockito.mockStatic(
            SessionUtils.class,
            invocation -> {
              Object result = invocation.callRealMethod();
              if (invocation.getMethod().getName().equals("getWithoutCommit")
                  && invocation.getArgument(0) == rootMapper
                  && (result instanceof TablePO
                      || result instanceof UserPO
                      || result instanceof GroupPO
                      || result instanceof RolePO
                      || result instanceof List
                          && !((List<?>) result).isEmpty()
                          && ((List<?>) result).get(0) instanceof RolePO)
                  && committed.compareAndSet(false, true)) {
                // Commit on another connection after the root SELECT, before any subsequent read.
                // The joined read already owns all its dependent rows.
                writer
                    .submit(
                        () -> {
                          write.run();
                          return null;
                        })
                    .get(30, TimeUnit.SECONDS);
              }
              return result;
            })) {
      T result = read.get();
      assertTrue(committed.get(), "the writer must commit at the observed read boundary");
      return result;
    } finally {
      writer.shutdownNow();
      assertTrue(writer.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @FunctionalInterface
  private interface CheckedRead<T> {
    T get() throws Exception;
  }

  @FunctionalInterface
  private interface CheckedWrite {
    void run() throws Exception;
  }
}
