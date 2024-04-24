/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.memory;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Entity.EntityType;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntitySerDe;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.TestCatalog;
import com.datastrato.gravitino.authorization.AuthorizationUtils;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.utils.Executable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestMemoryEntityStore {

  public static class InMemoryEntityStore implements EntityStore {
    private final Map<NameIdentifier, Entity> entityMap;
    private final Lock lock;

    public InMemoryEntityStore() {
      this.entityMap = Maps.newConcurrentMap();
      this.lock = new ReentrantLock();
    }

    public void clear() {
      entityMap.clear();
    }

    @Override
    public void initialize(Config config) throws RuntimeException {}

    @Override
    public void setSerDe(EntitySerDe entitySerDe) {}

    @Override
    public <E extends Entity & HasIdentifier> List<E> list(
        Namespace namespace, Class<E> cl, EntityType entityType) throws IOException {
      return entityMap.entrySet().stream()
          .filter(e -> e.getKey().namespace().equals(namespace))
          .map(entry -> (E) entry.getValue())
          .collect(Collectors.toList());
    }

    @Override
    public boolean exists(NameIdentifier nameIdentifier, EntityType type) throws IOException {
      return entityMap.containsKey(nameIdentifier);
    }

    @Override
    public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
        throws IOException, EntityAlreadyExistsException {
      NameIdentifier ident = e.nameIdentifier();
      if (overwritten) {
        entityMap.put(ident, e);
      } else {
        executeInTransaction(
            () -> {
              if (exists(e.nameIdentifier(), e.type())) {
                throw new EntityAlreadyExistsException("Entity " + ident + " already exists");
              }
              entityMap.put(ident, e);
              return null;
            });
      }
    }

    @Override
    public <E extends Entity & HasIdentifier> E update(
        NameIdentifier ident, Class<E> type, EntityType entityType, Function<E, E> updater)
        throws IOException, NoSuchEntityException {
      return executeInTransaction(
          () -> {
            E e = (E) entityMap.get(ident);
            if (e == null) {
              throw new NoSuchEntityException("Entity %s does not exist", ident);
            }

            E newE = updater.apply(e);
            NameIdentifier newIdent = NameIdentifier.of(newE.namespace(), newE.name());
            if (!newIdent.equals(ident)) {
              delete(ident, entityType);
            }
            entityMap.put(newIdent, newE);
            return newE;
          });
    }

    @Override
    public <E extends Entity & HasIdentifier> E get(
        NameIdentifier ident, EntityType entityType, Class<E> cl)
        throws NoSuchEntityException, IOException {
      E e = (E) entityMap.get(ident);
      if (e == null) {
        throw new NoSuchEntityException("Entity %s does not exist", ident);
      }

      return e;
    }

    @Override
    public boolean delete(NameIdentifier ident, EntityType entityType, boolean cascade)
        throws IOException {
      Entity prev = entityMap.remove(ident);
      return prev != null;
    }

    @Override
    public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
        throws E, IOException {
      try {
        lock.lock();
        return executable.execute();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void close() throws IOException {
      entityMap.clear();
    }
  }

  @Test
  public void testEntityStoreAndRetrieve() throws Exception {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(1L)
            .withName("metalake")
            .withAuditInfo(auditInfo)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(TestCatalog.Type.RELATIONAL)
            .withProvider("test")
            .withAuditInfo(auditInfo)
            .build();

    SchemaEntity schemaEntity =
        SchemaEntity.builder()
            .withId(1L)
            .withName("schema")
            .withNamespace(Namespace.of("metalake", "catalog"))
            .withAuditInfo(auditInfo)
            .build();

    TableEntity tableEntity =
        TableEntity.builder()
            .withId(1L)
            .withName("table")
            .withNamespace(Namespace.of("metalake", "catalog", "db"))
            .withAuditInfo(auditInfo)
            .build();

    FilesetEntity filesetEntity =
        FilesetEntity.builder()
            .withId(1L)
            .withName("fileset")
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocation("file:/tmp")
            .withNamespace(Namespace.of("metalake", "catalog", "db"))
            .withAuditInfo(auditInfo)
            .build();

    UserEntity userEntity =
        UserEntity.builder()
            .withId(1L)
            .withName("user")
            .withNamespace(Namespace.of("metalake", "catalog", "db"))
            .withAuditInfo(auditInfo)
            .withRoleNames(null)
            .build();

    GroupEntity groupEntity =
        GroupEntity.builder()
            .withId(1L)
            .withName("group")
            .withNamespace(AuthorizationUtils.ofGroupNamespace("metalake"))
            .withAuditInfo(auditInfo)
            .withRoleNames(null)
            .build();

    RoleEntity roleEntity =
        RoleEntity.builder()
            .withId(1L)
            .withName("role")
            .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
            .withAuditInfo(auditInfo)
            .withSecurableObject(SecurableObjects.ofCatalog("catalog"))
            .withPrivileges(Lists.newArrayList(Privileges.UseCatalog.get()))
            .build();

    InMemoryEntityStore store = new InMemoryEntityStore();
    store.initialize(Mockito.mock(Config.class));
    store.setSerDe(Mockito.mock(EntitySerDe.class));

    store.put(metalake);
    store.put(catalog);
    store.put(schemaEntity);
    store.put(tableEntity);
    store.put(filesetEntity);
    store.put(userEntity);
    store.put(groupEntity);
    store.put(roleEntity);

    Metalake retrievedMetalake =
        store.get(metalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class);
    Assertions.assertEquals(metalake, retrievedMetalake);

    CatalogEntity retrievedCatalog =
        store.get(catalog.nameIdentifier(), EntityType.CATALOG, CatalogEntity.class);
    Assertions.assertEquals(catalog, retrievedCatalog);

    SchemaEntity retrievedSchema =
        store.get(schemaEntity.nameIdentifier(), EntityType.SCHEMA, SchemaEntity.class);
    Assertions.assertEquals(schemaEntity, retrievedSchema);

    TableEntity retrievedTable =
        store.get(tableEntity.nameIdentifier(), EntityType.TABLE, TableEntity.class);
    Assertions.assertEquals(tableEntity, retrievedTable);

    FilesetEntity retrievedFileset =
        store.get(filesetEntity.nameIdentifier(), EntityType.FILESET, FilesetEntity.class);
    Assertions.assertEquals(filesetEntity, retrievedFileset);

    UserEntity retrievedUser =
        store.get(userEntity.nameIdentifier(), EntityType.USER, UserEntity.class);
    Assertions.assertEquals(userEntity, retrievedUser);

    GroupEntity retrievedGroup =
        store.get(groupEntity.nameIdentifier(), EntityType.GROUP, GroupEntity.class);
    Assertions.assertEquals(groupEntity, retrievedGroup);

    RoleEntity retrievedRole =
        store.get(roleEntity.nameIdentifier(), EntityType.ROLE, RoleEntity.class);
    Assertions.assertEquals(roleEntity, retrievedRole);

    store.delete(metalake.nameIdentifier(), EntityType.METALAKE);
    NameIdentifier id = metalake.nameIdentifier();
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> store.get(id, EntityType.METALAKE, BaseMetalake.class));

    Assertions.assertThrows(EntityAlreadyExistsException.class, () -> store.put(catalog, false));
    store.close();
  }
}
