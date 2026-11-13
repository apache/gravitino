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
package org.apache.gravitino.authorization;

import static org.apache.gravitino.authorization.Privilege.Name.USE_CATALOG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Principal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.CoreBackend;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises public role authorization through the real H2/MyBatis entity store. */
@CoreBackend.H2
public class TestJcasbinRoleSingleFlightH2 {
  private static final String METALAKE = "single_flight_metalake";
  private static final String CATALOG = "single_flight_catalog";
  private static final String ROLE = "single_flight_role";
  private static final String USER = "single_flight_user";
  private static final long ROLE_ID = 103L;
  private static final long CATALOG_ID = 102L;
  private static final Principal PRINCIPAL = () -> USER;
  private static final MetadataObject OBJECT =
      MetadataObjects.of(null, CATALOG, MetadataObject.Type.CATALOG);
  private static final AuditInfo AUDIT =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.EPOCH).build();

  @TempDir Path directory;

  private Object previousConfig;
  private Object previousStore;
  private Object previousIdGenerator;
  private Object previousMetrics;
  private EntityIdResolver previousIdResolver;
  private CountingStore store;
  private JcasbinAuthorizer authorizer;

  @BeforeEach
  void setUp() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    previousConfig = FieldUtils.readField(env, "config", true);
    previousStore = FieldUtils.readField(env, "entityStore", true);
    previousIdGenerator = FieldUtils.readField(env, "idGenerator", true);
    previousMetrics = FieldUtils.readField(env, "metricsSystem", true);
    previousIdResolver =
        (EntityIdResolver)
            FieldUtils.readStaticField(EntityIdService.class, "entityIdResolver", true);

    // Like the core backend lane, this fixture owns one process-global MyBatis database at a
    // time. Refuse to borrow an existing factory: its data source could belong to another test,
    // and closing an unknown live backend merely to make this setup pass would be unsafe.
    assertNull(
        FieldUtils.readStaticField(SqlSessionFactoryHelper.class, "sqlSessionFactory", true),
        "The isolated H2 fixture must not reuse another test's MyBatis factory");

    Config config = new Config(false) {};
    String dbPath = directory.resolve("roles").toAbsolutePath().toString();
    config.set(Configs.ENTITY_STORE, Configs.RELATIONAL_ENTITY_STORE);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH, dbPath);
    config.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        "jdbc:h2:file:" + dbPath + ";DB_CLOSE_DELAY=-1;MODE=MYSQL;AUTO_SERVER=FALSE");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "test");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "test");
    // Count real reads, rather than hits in the EntityStore cache. The authorizer keeps its
    // production caches and the relational implementation still executes normal MyBatis queries.
    config.set(Configs.CACHE_ENABLED, false);
    config.set(Configs.GRAVITINO_AUTHORIZATION_CHANGE_POLL_INTERVAL_SECS, 3600L);
    config.set(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS, 3600L);
    FieldUtils.writeField(env, "config", config, true);
    FieldUtils.writeField(env, "idGenerator", RandomIdGenerator.INSTANCE, true);
    FieldUtils.writeField(env, "metricsSystem", null, true);

    store = new CountingStore();
    store.initialize(config);
    FieldUtils.writeField(env, "entityStore", store, true);
    seedEntities();
    authorizer = new JcasbinAuthorizer();
    authorizer.initialize();
    store.resetCounts();
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      if (store != null) {
        store.releaseRoleReads.countDown();
      }
      if (authorizer != null) {
        authorizer.close();
      }
    } finally {
      try {
        if (store != null) {
          store.close();
        }
      } finally {
        GravitinoEnv env = GravitinoEnv.getInstance();
        FieldUtils.writeField(env, "config", previousConfig, true);
        FieldUtils.writeField(env, "entityStore", previousStore, true);
        FieldUtils.writeField(env, "idGenerator", previousIdGenerator, true);
        FieldUtils.writeField(env, "metricsSystem", previousMetrics, true);
        EntityIdService.initialize(previousIdResolver);
      }
    }
  }

  @Test
  void testConcurrentRequestsShareOneRealRoleRead() throws Exception {
    int requests = 4;
    GravitinoCache<Long, Long> loaded = loadedRoles();
    FieldUtils.writeField(authorizer, "loadedRoles", new FirstMissBarrier(loaded, requests), true);
    store.blockRoleReads = true;
    ExecutorService executor = Executors.newFixedThreadPool(requests);
    List<Future<Boolean>> results = new ArrayList<>();
    try {
      for (int i = 0; i < requests; i++) {
        results.add(executor.submit(this::authorize));
      }
      assertTrue(store.firstRoleRead.await(15, TimeUnit.SECONDS));
      store.releaseRoleReads.countDown();
      for (Future<Boolean> result : results) {
        assertTrue(result.get(20, TimeUnit.SECONDS));
      }
      assertEquals(
          1, store.roleBatchReads.get(), "one same-role load must serve the stale request wave");
      assertEquals(1, store.roleEntitiesRequested.get());
      assertEquals(0, store.rolePointReads.get(), "the real relational batch path is supported");
      assertTrue(store.catalogReads.get() >= 1, "role policies must resolve a real catalog entity");
      assertEquals(
          List.of(
              List.of(
                  Long.toString(ROLE_ID),
                  "CATALOG",
                  Long.toString(CATALOG_ID),
                  "USE_CATALOG",
                  "allow")),
          policies());
    } finally {
      store.releaseRoleReads.countDown();
      for (Future<Boolean> result : results) {
        result.cancel(true);
      }
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(20, TimeUnit.SECONDS));
    }
  }

  @Test
  void testPersistedRevocationReloadsPolicies() throws Exception {
    assertTrue(authorize());
    assertEquals(1, store.roleBatchReads.get());
    long originalVersion = roleVersion();

    store.update(
        NameIdentifier.of(AuthorizationUtils.ofRoleNamespace(METALAKE), ROLE),
        RoleEntity.class,
        Entity.EntityType.ROLE,
        old -> role(List.of()));
    assertTrue(roleVersion() > originalVersion, "the MyBatis update must advance the role version");
    assertFalse(authorize(), "a fresh request must not retain the revoked catalog privilege");
    assertEquals(2, store.roleBatchReads.get());
    assertTrue(policies().isEmpty());
  }

  private boolean authorize() {
    // Every caller owns a request context, matching concurrent HTTP requests rather than workers
    // sharing one context's initializer.
    return authorizer.authorize(
        PRINCIPAL, METALAKE, OBJECT, USE_CATALOG, new AuthorizationRequestContext());
  }

  private long roleVersion() {
    return SessionUtils.getWithoutCommit(
            RoleMetaMapper.class, mapper -> mapper.batchGetRoleUpdatedAt(List.of(ROLE_ID)))
        .get(0)
        .getUpdatedAt();
  }

  @SuppressWarnings("unchecked")
  private GravitinoCache<Long, Long> loadedRoles() throws IllegalAccessException {
    return (GravitinoCache<Long, Long>) FieldUtils.readField(authorizer, "loadedRoles", true);
  }

  @SuppressWarnings("unchecked")
  private List<List<String>> policies() throws Exception {
    Object enforcer = FieldUtils.readField(authorizer, "allowEnforcer", true);
    return (List<List<String>>) enforcer.getClass().getMethod("getPolicy").invoke(enforcer);
  }

  private void seedEntities() throws IOException {
    store.put(
        BaseMetalake.builder()
            .withId(101L)
            .withName(METALAKE)
            .withComment("")
            .withProperties(null)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AUDIT)
            .build(),
        false);
    store.put(
        CatalogEntity.builder()
            .withId(CATALOG_ID)
            .withName(CATALOG)
            .withNamespace(Namespace.of(METALAKE))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withComment("")
            .withProperties(null)
            .withAuditInfo(AUDIT)
            .build(),
        false);
    store.put(
        role(List.of(SecurableObjects.ofCatalog(CATALOG, List.of(Privileges.UseCatalog.allow())))),
        false);
    store.put(
        UserEntity.builder()
            .withId(104L)
            .withName(USER)
            .withNamespace(AuthorizationUtils.ofUserNamespace(METALAKE))
            .withRoleIds(List.of(ROLE_ID))
            .withRoleNames(List.of(ROLE))
            .withAuditInfo(AUDIT)
            .build(),
        false);
  }

  private static RoleEntity role(List<SecurableObject> objects) {
    return RoleEntity.builder()
        .withId(ROLE_ID)
        .withName(ROLE)
        .withNamespace(AuthorizationUtils.ofRoleNamespace(METALAKE))
        .withProperties(null)
        .withAuditInfo(AUDIT)
        .withSecurableObjects(objects)
        .build();
  }

  /** All reads below delegate to the actual relational store, including its MyBatis backend. */
  private static class CountingStore extends RelationalEntityStore {
    final AtomicInteger roleBatchReads = new AtomicInteger();
    final AtomicInteger roleEntitiesRequested = new AtomicInteger();
    final AtomicInteger rolePointReads = new AtomicInteger();
    final AtomicInteger catalogReads = new AtomicInteger();
    final CountDownLatch firstRoleRead = new CountDownLatch(1);
    final CountDownLatch releaseRoleReads = new CountDownLatch(1);
    volatile boolean blockRoleReads;

    void resetCounts() {
      roleBatchReads.set(0);
      roleEntitiesRequested.set(0);
      rolePointReads.set(0);
      catalogReads.set(0);
    }

    @Override
    public <E extends Entity & HasIdentifier> List<E> batchGet(
        List<NameIdentifier> identifiers, Entity.EntityType type, Class<E> entityClass) {
      if (type == Entity.EntityType.ROLE) {
        roleBatchReads.incrementAndGet();
        roleEntitiesRequested.addAndGet(identifiers.size());
        firstRoleRead.countDown();
        if (blockRoleReads) {
          try {
            if (!releaseRoleReads.await(20, TimeUnit.SECONDS)) {
              throw new IllegalStateException("Timed out waiting to release the real role read");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted real role read", e);
          }
        }
      }
      return super.batchGet(identifiers, type, entityClass);
    }

    @Override
    public <E extends Entity & HasIdentifier> E get(
        NameIdentifier identifier, Entity.EntityType type, Class<E> entityClass)
        throws IOException {
      if (type == Entity.EntityType.ROLE) {
        rolePointReads.incrementAndGet();
      } else if (type == Entity.EntityType.CATALOG) {
        catalogReads.incrementAndGet();
      }
      return super.get(identifier, type, entityClass);
    }
  }

  /** Freeze each worker's first cache miss before any worker can populate the role policies. */
  private static class FirstMissBarrier implements GravitinoCache<Long, Long> {
    private final GravitinoCache<Long, Long> delegate;
    private final CyclicBarrier barrier;
    private final Set<Thread> seen = ConcurrentHashMap.newKeySet();

    FirstMissBarrier(GravitinoCache<Long, Long> delegate, int participants) {
      this.delegate = delegate;
      barrier = new CyclicBarrier(participants);
    }

    @Override
    public Optional<Long> getIfPresent(Long key) {
      Optional<Long> value = delegate.getIfPresent(key);
      if (key == ROLE_ID && value.isEmpty() && seen.add(Thread.currentThread())) {
        try {
          barrier.await(15, TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new IllegalStateException(
              "Concurrent requests did not reach their first role miss", e);
        }
      }
      return value;
    }

    @Override
    public Long get(Long key, Function<Long, Long> loader) {
      return delegate.get(key, loader);
    }

    @Override
    public void put(Long key, Long value) {
      delegate.put(key, value);
    }

    @Override
    public void invalidate(Long key) {
      delegate.invalidate(key);
    }

    @Override
    public void invalidateAll() {
      delegate.invalidateAll();
    }

    @Override
    public void invalidateByPrefix(String prefix) {
      delegate.invalidateByPrefix(prefix);
    }

    @Override
    public void runInvalidationBatch(Runnable batch) {
      delegate.runInvalidationBatch(batch);
    }

    @Override
    public long size() {
      return delegate.size();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
