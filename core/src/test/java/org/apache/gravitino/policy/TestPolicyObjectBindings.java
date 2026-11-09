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
package org.apache.gravitino.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Tests internal policy bindings through the entity store and relation abstractions. */
public class TestPolicyObjectBindings extends TestJDBCBackend {
  private static final String METALAKE = "binding_test";
  private Connection connection;
  private EntityStore store;
  private PolicyManager manager;
  private PolicyEntity policy;
  private CatalogEntity catalog;
  private TagEntity tag;

  /** Creates a policy with its internal tag and opens a transaction for assignment checks. */
  @BeforeEach
  public void preparePolicy() throws Exception {
    store = createStore();
    manager = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    createAndInsertMakeLake(METALAKE);
    catalog = createAndInsertCatalog(METALAKE, "catalog");
    policy = createPolicy(20L, Namespace.of(METALAKE), "policy", AUDIT_INFO);
    store.put(policy, false);
    tag =
        store.get(
            NameIdentifierUtil.ofTag(METALAKE, HiddenPolicyTag.name(policy.id())),
            Entity.EntityType.TAG,
            TagEntity.class);
    beginTransaction();
  }

  /** Rolls back the caller's transaction without closing the shared test backend. */
  @AfterEach
  public void rollbackTransaction() {
    SessionUtils.rollbackTransaction();
  }

  /** Only the assignment participates in rollback; the policy-created tag and link are reused. */
  @TestTemplate
  public void testIdempotentBindingAndRollback() throws Exception {
    assertEquals(tag.id(), addPolicy(policy.name(), catalog.name(), AUDIT_INFO));
    assertEquals(tag.id(), addPolicy(policy.name(), catalog.name(), AUDIT_INFO));
    assertCounts(1, 1, 1);
    SessionUtils.rollbackTransaction();
    beginTransaction();
    assertCounts(1, 1, 0);
  }

  /** Binding a policy-created internal tag commits the assignment without an outer transaction. */
  @TestTemplate
  public void testBindingWithoutOuterTransaction() throws Exception {
    SessionUtils.rollbackTransaction();
    assertEquals(tag.id(), addPolicy(policy.name(), catalog.name(), AUDIT_INFO));
    assertEquals(tag.id(), addPolicy(policy.name(), catalog.name(), AUDIT_INFO));
    beginTransaction();
    assertCounts(1, 1, 1);
  }

  /** Concurrent callers assign the existing hidden tag to different objects. */
  @TestTemplate
  public void testConcurrentBindings() throws Exception {
    SessionUtils.rollbackTransaction();
    CatalogEntity second = createAndInsertCatalog(METALAKE, "second_catalog");
    PolicyManager concurrentManager = new PolicyManager(RandomIdGenerator.INSTANCE, createStore());
    CountDownLatch bothStarted = new CountDownLatch(2);
    ExecutorService writers = Executors.newFixedThreadPool(2);
    try {
      Future<Long> firstBinding =
          writers.submit(
              () -> {
                bothStarted.countDown();
                assertTrue(bothStarted.await(15, TimeUnit.SECONDS));
                return concurrentManager.addPolicyToObject(
                    METALAKE,
                    policy.name(),
                    MetadataObjects.parse(catalog.name(), MetadataObject.Type.CATALOG),
                    AUDIT_INFO);
              });
      Future<Long> secondBinding =
          writers.submit(
              () -> {
                bothStarted.countDown();
                assertTrue(bothStarted.await(15, TimeUnit.SECONDS));
                return concurrentManager.addPolicyToObject(
                    METALAKE,
                    policy.name(),
                    MetadataObjects.parse(second.name(), MetadataObject.Type.CATALOG),
                    AUDIT_INFO);
              });
      assertEquals(tag.id(), firstBinding.get(30, TimeUnit.SECONDS));
      assertEquals(tag.id(), secondBinding.get(30, TimeUnit.SECONDS));
      beginTransaction();
      assertCounts(1, 1, 2);
    } finally {
      writers.shutdownNow();
      assertTrue(writers.awaitTermination(15, TimeUnit.SECONDS));
    }
  }

  /** A missing tag is reported without creating a replacement or any relations. */
  @TestTemplate
  public void testRejectsMissingHiddenTag() throws Exception {
    store.delete(tag.nameIdentifier(), Entity.EntityType.TAG);
    assertThrows(
        NoSuchEntityException.class, () -> addPolicy(policy.name(), catalog.name(), AUDIT_INFO));
    assertCounts(0, 0, 0);
  }

  /** A failed assignment preserves the existing tag and policy link and can be retried. */
  @TestTemplate
  public void testRetriesFailedAssignmentWithoutOuterTransaction() throws Exception {
    SessionUtils.rollbackTransaction();
    RelationalEntityStore failingStore = spy((RelationalEntityStore) store);
    IOException failure = new IOException("Assignment failed");
    doAnswer(
            invocation -> {
              throw failure;
            })
        .when(failingStore)
        .updateEntityRelations(any(RelationUpdate.class));
    PolicyManager failingManager = new PolicyManager(RandomIdGenerator.INSTANCE, failingStore);
    assertSame(
        failure,
        assertThrows(
            IOException.class,
            () ->
                failingManager.addPolicyToObject(
                    METALAKE,
                    policy.name(),
                    MetadataObjects.parse(catalog.name(), MetadataObject.Type.CATALOG),
                    AUDIT_INFO)));
    beginTransaction();
    assertCounts(1, 1, 0);
    assertEquals(tag, store.get(tag.nameIdentifier(), Entity.EntityType.TAG, TagEntity.class));
    SessionUtils.rollbackTransaction();

    assertEquals(tag.id(), addPolicy(policy.name(), catalog.name(), AUDIT_INFO));
    beginTransaction();
    assertCounts(1, 1, 1);
  }

  /** Missing endpoints, unsupported types and missing audit metadata fail before writes. */
  @TestTemplate
  public void testRejectsInvalidEndpoints() throws Exception {
    assertThrows(
        NoSuchEntityException.class, () -> addPolicy("missing", catalog.name(), AUDIT_INFO));
    assertThrows(
        NoSuchEntityException.class, () -> addPolicy(policy.name(), "missing", AUDIT_INFO));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            manager.addPolicyToObject(
                METALAKE,
                policy.name(),
                MetadataObjects.parse("policy", MetadataObject.Type.POLICY),
                AUDIT_INFO));
    assertThrows(
        IllegalArgumentException.class, () -> addPolicy(policy.name(), catalog.name(), null));
    assertCounts(1, 1, 0);
  }

  /** Assignment audit metadata is preserved on retries; the existing policy link is untouched. */
  @TestTemplate
  public void testPreservesRelationAudit() throws Exception {
    AuditInfo audit =
        AuditInfo.builder()
            .withCreator("original-user")
            .withCreateTime(Instant.parse("2020-01-02T03:04:05Z"))
            .build();
    addPolicy(policy.name(), catalog.name(), audit);
    addPolicy(policy.name(), catalog.name(), AUDIT_INFO);
    for (String table : new String[] {"tag_relation_meta", "policy_tag_relation_meta"}) {
      try (Statement statement = connection.createStatement();
          ResultSet rows =
              statement.executeQuery("SELECT audit_info FROM " + table + " WHERE deleted_at = 0")) {
        assertTrue(rows.next());
        assertEquals(
            JsonUtils.anyFieldMapper()
                .writeValueAsString(table.equals("tag_relation_meta") ? audit : AUDIT_INFO),
            rows.getString(1));
      }
    }
  }

  private long addPolicy(String name, String object, AuditInfo audit) throws Exception {
    return manager.addPolicyToObject(
        METALAKE, name, MetadataObjects.parse(object, MetadataObject.Type.CATALOG), audit);
  }

  private void beginTransaction() throws SQLException {
    SessionUtils.beginTransaction();
    connection = SqlSessions.currentConnection();
  }

  private void assertCounts(long tags, long policyRelations, long assignments) throws Exception {
    assertEquals(tags, count("tag_meta"));
    assertEquals(policyRelations, count("policy_tag_relation_meta"));
    assertEquals(assignments, count("tag_relation_meta"));
  }

  private long count(String table) throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT COUNT(*) FROM " + table + " WHERE deleted_at = 0")) {
      rows.next();
      return rows.getLong(1);
    }
  }

  private RelationalEntityStore createStore() throws IllegalAccessException {
    RelationalEntityStore store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", new NoOpsCache(new Config(false) {}), true);
    return store;
  }
}
