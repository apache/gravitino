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
package org.apache.gravitino.storage.relational;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A cacheable write inside an outer SQL transaction must stay invisible to other sessions until
 * that transaction commits, and must not be cached at all if it rolls back. Update and delete keep
 * the previous cache entry blocked until the outermost session closes, so a peer cannot refill it
 * in the gap between the backend call and the outer commit.
 */
public class TestRelationalEntityStoreUncommittedCache {

  private static final String METALAKE = "metalake_uncommitted_cache";
  private static final String USER = "root";
  private static final String PASSWORD = "123456";

  private static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  private RelationalEntityStore store;
  private EntityCache cache;
  private String dbPath;
  private Object previousConfig;
  private Object previousIdGenerator;
  private ExecutorService peer;

  @BeforeEach
  void setUp() throws Exception {
    peer =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "uncommitted-cache-peer");
              thread.setDaemon(true);
              return thread;
            });
    initH2Store();
    store.put(metalake(), false);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (SessionUtils.isInTransaction()) {
      SessionUtils.rollbackTransaction();
    }
    if (peer != null) {
      peer.shutdownNow();
      peer.awaitTermination(10, TimeUnit.SECONDS);
    }
    if (dbPath != null) {
      try {
        store.close();
      } finally {
        FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
        FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", previousIdGenerator, true);
        FileUtils.deleteDirectory(new File(dbPath));
        dbPath = null;
      }
    }
    store = null;
    cache = null;
  }

  @Test
  void testInsertInsideOpenTransactionIsInvisibleUntilRollback() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "rollback_without_read");
    SessionUtils.beginTransaction();
    try {
      store.put(policy(ident), false);
      assertInvisible(ident);
      SessionUtils.rollbackTransaction();
    } finally {
      rollbackIfOpen();
    }
    assertInvisible(ident);
  }

  @Test
  void testReadBackInsideOpenTransactionDoesNotPublishToOtherSessions() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "rollback_after_read");
    SessionUtils.beginTransaction();
    try {
      store.put(policy(ident), false);
      PolicyEntity loaded = store.get(ident, Entity.EntityType.POLICY, PolicyEntity.class);
      Assertions.assertEquals(ident.name(), loaded.name());
      List<PolicyEntity> batch =
          store.batchGet(List.of(ident), Entity.EntityType.POLICY, PolicyEntity.class);
      Assertions.assertEquals(1, batch.size());
      Assertions.assertEquals(ident.name(), batch.get(0).name());
      assertInvisible(ident);
      SessionUtils.rollbackTransaction();
    } finally {
      rollbackIfOpen();
    }
    assertInvisible(ident);
  }

  @Test
  void testCommittedInsertFillsCacheOnTheNextRead() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "commit_then_read");
    SessionUtils.beginTransaction();
    try {
      store.put(policy(ident), false);
      Assertions.assertFalse(cached(ident));
      SessionUtils.commitTransaction();
    } finally {
      rollbackIfOpen();
    }

    Assertions.assertFalse(cached(ident), "commit itself does not publish the entity");
    Assertions.assertTrue(peerCanRead(ident));
    Assertions.assertTrue(cached(ident), "the first read after commit fills the cache");
  }

  @Test
  void testStandalonePutStillWarmsTheCache() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "standalone");
    store.put(policy(ident), false);
    Assertions.assertTrue(cached(ident));
    Assertions.assertTrue(peerCanRead(ident));
  }

  @Test
  void testNestedUpdateDoesNotRepublishTheOldRowBeforeCommit() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "nested_update");
    store.put(policy(ident), false);
    Assertions.assertTrue(peerCanRead(ident));
    Assertions.assertTrue(cached(ident));

    SessionUtils.beginTransaction();
    try {
      store.update(
          ident,
          PolicyEntity.class,
          Entity.EntityType.POLICY,
          current -> withComment(current, "changed"));
      Assertions.assertFalse(cached(ident));
      Assertions.assertTrue(writeBlocked(ident));
      PolicyEntity during = peerGet(ident);
      Assertions.assertEquals("", during.comment());
      Assertions.assertFalse(cached(ident), "a peer must not cache the pre-commit row");

      SessionUtils.beginTransaction();
      SessionUtils.commitTransaction();
      Assertions.assertTrue(writeBlocked(ident), "an inner commit must not release the block");
      Assertions.assertEquals("", peerGet(ident).comment());
      Assertions.assertFalse(cached(ident));

      SessionUtils.commitTransaction();
    } finally {
      rollbackIfOpen();
    }

    Assertions.assertFalse(cached(ident));
    Assertions.assertFalse(writeBlocked(ident));
    Assertions.assertEquals("changed", peerGet(ident).comment());
    Assertions.assertTrue(cached(ident));
  }

  @Test
  void testNestedUpdateRollbackKeepsTheCommittedRow() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "nested_update_rollback");
    store.put(policy(ident), false);
    Assertions.assertTrue(peerCanRead(ident));

    SessionUtils.beginTransaction();
    try {
      store.update(
          ident,
          PolicyEntity.class,
          Entity.EntityType.POLICY,
          current -> withComment(current, "rolled-back"));
      Assertions.assertFalse(cached(ident));
      SessionUtils.rollbackTransaction();
    } finally {
      rollbackIfOpen();
    }

    Assertions.assertEquals("", peerGet(ident).comment());
    Assertions.assertTrue(cached(ident));
  }

  @Test
  void testNestedRenameStaysBlockedUntilTheOuterSessionCloses() throws Exception {
    NameIdentifier oldIdent = NameIdentifierUtil.ofPolicy(METALAKE, "rename_old");
    NameIdentifier newIdent = NameIdentifierUtil.ofPolicy(METALAKE, "rename_new");
    store.put(policy(oldIdent), false);
    Assertions.assertTrue(peerCanRead(oldIdent));

    SessionUtils.beginTransaction();
    try {
      store.update(
          oldIdent,
          PolicyEntity.class,
          Entity.EntityType.POLICY,
          current -> withName(current, newIdent.name()));
      Assertions.assertTrue(writeBlocked(oldIdent));
      Assertions.assertTrue(writeBlocked(newIdent));
      Assertions.assertEquals("", peerGet(oldIdent).comment());
      Assertions.assertFalse(cached(oldIdent));
      Assertions.assertFalse(peerCanRead(newIdent));
      SessionUtils.rollbackTransaction();
    } finally {
      rollbackIfOpen();
    }

    Assertions.assertFalse(writeBlocked(oldIdent));
    Assertions.assertFalse(writeBlocked(newIdent));
    Assertions.assertTrue(peerCanRead(oldIdent));
    Assertions.assertFalse(peerCanRead(newIdent));
  }

  @Test
  void testNestedDeleteDoesNotRepublishTheRowBeforeCommit() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "nested_delete");
    store.put(policy(ident), false);
    Assertions.assertTrue(peerCanRead(ident));

    SessionUtils.beginTransaction();
    try {
      Assertions.assertTrue(store.delete(ident, Entity.EntityType.POLICY, false));
      Assertions.assertFalse(cached(ident));
      Assertions.assertTrue(peerExists(ident));
      Assertions.assertFalse(cached(ident));
      SessionUtils.commitTransaction();
    } finally {
      rollbackIfOpen();
    }

    Assertions.assertFalse(peerExists(ident));
    Assertions.assertFalse(cached(ident));
    Assertions.assertFalse(peerCanRead(ident));
  }

  @Test
  void testNestedDeleteRollbackLeavesTheRowCacheable() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "nested_delete_rollback");
    store.put(policy(ident), false);
    Assertions.assertTrue(peerCanRead(ident));

    SessionUtils.beginTransaction();
    try {
      Assertions.assertTrue(store.delete(ident, Entity.EntityType.POLICY, false));
      Assertions.assertTrue(peerExists(ident));
      Assertions.assertFalse(cached(ident));
      SessionUtils.rollbackTransaction();
    } finally {
      rollbackIfOpen();
    }

    Assertions.assertTrue(peerCanRead(ident));
    Assertions.assertTrue(cached(ident));
  }

  @Test
  void testStandaloneUpdateReleasesTheBlockWhenItReturns() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "standalone_update");
    store.put(policy(ident), false);
    store.update(
        ident,
        PolicyEntity.class,
        Entity.EntityType.POLICY,
        current -> withComment(current, "now"));
    Assertions.assertFalse(writeBlocked(ident));
    Assertions.assertFalse(cached(ident));
    Assertions.assertEquals("now", peerGet(ident).comment());
    Assertions.assertTrue(cached(ident));
  }

  @Test
  void testRequestEndRollbackDropsAnUncommittedInsert() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "request_end_rollback");
    SessionUtils.beginTransaction();
    store.put(policy(ident), false);
    Assertions.assertTrue(writeBlocked(ident));
    while (SqlSessions.hasActiveSession()) {
      SqlSessions.rollbackAndCloseSqlSession();
    }
    Assertions.assertFalse(SessionUtils.isInTransaction());
    Assertions.assertFalse(writeBlocked(ident));
    assertInvisible(ident);
  }

  @Test
  void testDeadOwnerReleasesTheBlockOnTheNextRead() throws Exception {
    NameIdentifier ident = NameIdentifierUtil.ofPolicy(METALAKE, "dead_owner");
    store.put(policy(ident), false);
    Assertions.assertTrue(peerCanRead(ident));

    Thread owner =
        new Thread(
            () -> {
              SessionUtils.beginTransaction();
              try {
                store.update(
                    ident,
                    PolicyEntity.class,
                    Entity.EntityType.POLICY,
                    current -> withComment(current, "committed-without-close"));
                SqlSessions.currentConnection().commit();
              } catch (IOException | SQLException e) {
                throw new IllegalStateException(e);
              }
            },
            "dead-pending-owner");
    owner.start();
    owner.join();
    Assertions.assertFalse(owner.isAlive());
    Assertions.assertTrue(writeBlocked(ident), "the dead thread never reached session close");

    Assertions.assertEquals("committed-without-close", peerGet(ident).comment());
    Assertions.assertFalse(writeBlocked(ident));
    Assertions.assertTrue(cached(ident));
  }

  private void assertInvisible(NameIdentifier ident) throws Exception {
    Assertions.assertFalse(cached(ident));
    Assertions.assertFalse(peerExists(ident));
    Assertions.assertFalse(peerCanRead(ident));
  }

  private boolean cached(NameIdentifier ident) {
    return cache.contains(ident, Entity.EntityType.POLICY);
  }

  private boolean peerExists(NameIdentifier ident) throws Exception {
    return peer.submit(() -> store.exists(ident, Entity.EntityType.POLICY))
        .get(30, TimeUnit.SECONDS);
  }

  private PolicyEntity peerGet(NameIdentifier ident) throws Exception {
    return peer.submit(() -> store.get(ident, Entity.EntityType.POLICY, PolicyEntity.class))
        .get(30, TimeUnit.SECONDS);
  }

  private boolean peerCanRead(NameIdentifier ident) throws Exception {
    return peer.submit(
            () -> {
              try {
                store.get(ident, Entity.EntityType.POLICY, PolicyEntity.class);
                return true;
              } catch (NoSuchEntityException expected) {
                return false;
              }
            })
        .get(30, TimeUnit.SECONDS);
  }

  private static void rollbackIfOpen() {
    if (SessionUtils.isInTransaction()) {
      SessionUtils.rollbackTransaction();
    }
  }

  @SuppressWarnings("unchecked")
  private boolean writeBlocked(NameIdentifier ident) throws IllegalAccessException {
    Map<String, AtomicInteger> blocks =
        (Map<String, AtomicInteger>) FieldUtils.readField(store, "cacheBlocks", true);
    AtomicInteger count = blocks.get("w\0" + Entity.EntityType.POLICY.name() + "\0" + ident);
    return count != null && count.get() > 0;
  }

  private static PolicyEntity withComment(PolicyEntity current, String comment) {
    return copy(current, current.name(), comment);
  }

  private static PolicyEntity withName(PolicyEntity current, String name) {
    return copy(current, name, current.comment());
  }

  private static PolicyEntity copy(PolicyEntity current, String name, String comment) {
    return PolicyEntity.builder()
        .withId(current.id())
        .withNamespace(current.namespace())
        .withName(name)
        .withPolicyType(current.policyType())
        .withComment(comment)
        .withEnabled(current.enabled())
        .withContent(current.content())
        .withAuditInfo((AuditInfo) current.auditInfo())
        .build();
  }

  private void initH2Store() throws IOException, IllegalAccessException {
    dbPath = Files.createTempDirectory("gravitino_uncommitted_cache_").toString();
    String jdbcUrl = String.format("jdbc:h2:file:%s;MODE=MYSQL", new File(dbPath, "db").getPath());

    Config config = new Config(false) {};
    config.set(Configs.ENTITY_STORE, Configs.RELATIONAL_ENTITY_STORE);
    config.set(Configs.ENTITY_RELATIONAL_STORE, Configs.DEFAULT_ENTITY_RELATIONAL_STORE);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, jdbcUrl);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, USER);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, PASSWORD);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    config.set(Configs.CACHE_ENABLED, true);

    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    previousIdGenerator = FieldUtils.readField(GravitinoEnv.getInstance(), "idGenerator", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "idGenerator", RandomIdGenerator.INSTANCE, true);

    store = new RelationalEntityStore();
    store.initialize(config);
    cache = (EntityCache) FieldUtils.readField(store, "cache", true);
  }

  private static BaseMetalake metalake() {
    return BaseMetalake.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(METALAKE)
        .withAuditInfo(AUDIT_INFO)
        .withComment("")
        .withProperties(null)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  private static PolicyEntity policy(NameIdentifier ident) {
    return PolicyEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withNamespace(ident.namespace())
        .withName(ident.name())
        .withPolicyType(Policy.BuiltInType.CUSTOM)
        .withComment("")
        .withEnabled(true)
        .withContent(
            PolicyContents.custom(
                ImmutableMap.of("field", 1),
                ImmutableSet.of(MetadataObject.Type.CATALOG, MetadataObject.Type.TABLE),
                null))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }
}
