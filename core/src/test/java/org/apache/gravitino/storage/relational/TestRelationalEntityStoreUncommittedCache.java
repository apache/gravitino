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
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A cacheable insert must stay invisible to other SQL sessions until the outer transaction commits,
 * including when the writer reads the row back through {@code get} or {@code batchGet} before the
 * transaction ends.
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
