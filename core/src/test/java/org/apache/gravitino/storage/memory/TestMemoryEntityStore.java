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
package org.apache.gravitino.storage.memory;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.google.common.collect.ImmutableMap;
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.Executable;
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
                throw new EntityAlreadyExistsException("Entity %s already exists", ident);
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
      lock.lock();
      Map<NameIdentifier, Entity> snapshot = createSnapshot();
      try {
        return executable.execute();
      } catch (Exception e) {
        if (snapshot != null) {
          // restore the entityMap in case of failed transactions
          entityMap.clear();
          entityMap.putAll(snapshot);
        }
        throw e;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public int batchDelete(List<Pair<NameIdentifier, EntityType>> entitiesToDelete, boolean cascade)
        throws IOException {
      throw new UnsupportedOperationException(
          "Batch delete is not supported in InMemoryEntityStore.");
    }

    @Override
    public <E extends Entity & HasIdentifier> void batchPut(List<E> entities, boolean overwritten)
        throws IOException, EntityAlreadyExistsException {
      throw new UnsupportedOperationException("Batch put is not supported in InMemoryEntityStore.");
    }

    @Override
    public void close() throws IOException {
      entityMap.clear();
    }

    public Map<NameIdentifier, Entity> createSnapshot() {
      return entityMap.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, Maps::newHashMap));
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
            .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "file:/tmp"))
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
            .withSecurableObjects(
                Lists.newArrayList(
                    SecurableObjects.ofCatalog(
                        "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))))
            .build();

    InMemoryEntityStore store = new InMemoryEntityStore();
    store.initialize(Mockito.mock(Config.class));

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
