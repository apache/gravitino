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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.SupportsRelationOperations;
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

  /** Creates a legacy policy without a hidden tag and opens the migration transaction. */
  @BeforeEach
  public void preparePolicy() throws Exception {
    store = new RelationalEntityStore(backend, new Config(false) {});
    manager = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    createAndInsertMakeLake(METALAKE);
    catalog = createAndInsertCatalog(METALAKE, "catalog");
    policy = createPolicy(20L, Namespace.of(METALAKE), "policy", AUDIT_INFO);
    store.put(policy, false);
    store.delete(
        NameIdentifierUtil.ofTag(METALAKE, HiddenPolicyTag.name(policy.id())),
        Entity.EntityType.TAG);
    SessionUtils.beginTransaction();
    connection = SqlSessions.currentConnection();
  }

  /** Rolls back the caller's transaction without closing the shared test backend. */
  @AfterEach
  public void rollbackTransaction() {
    SessionUtils.rollbackTransaction();
  }

  /** New tags and both relations are idempotent and participate in the caller's transaction. */
  @TestTemplate
  public void testIdempotentBindingAndRollback() throws Exception {
    long tagId = addPolicy(policy.name(), catalog.name(), AUDIT_INFO);
    assertEquals(tagId, addPolicy(policy.name(), catalog.name(), AUDIT_INFO));
    assertEquals(1, count("tag_meta"));
    assertEquals(1, count("policy_tag_relation_meta"));
    assertEquals(1, count("tag_relation_meta"));
    assertTrue(
        store
            .get(
                NameIdentifierUtil.ofTag(METALAKE, HiddenPolicyTag.name(policy.id())),
                Entity.EntityType.TAG,
                TagEntity.class)
            .hidden());
    SessionUtils.rollbackTransaction();
    SessionUtils.beginTransaction();
    connection = SqlSessions.currentConnection();
    assertEquals(0, count("tag_meta"));
    assertEquals(0, count("policy_tag_relation_meta"));
    assertEquals(0, count("tag_relation_meta"));
  }

  /** Policy-created tags are reused and remain when only the new assignment rolls back. */
  @TestTemplate
  public void testReusesPolicyTag() throws Exception {
    SessionUtils.rollbackTransaction();
    PolicyEntity second = createPolicy(21L, policy.namespace(), "second_policy", AUDIT_INFO);
    store.put(second, false);
    TagEntity tag =
        store.get(
            NameIdentifierUtil.ofTag(METALAKE, HiddenPolicyTag.name(second.id())),
            Entity.EntityType.TAG,
            TagEntity.class);
    SessionUtils.beginTransaction();
    connection = SqlSessions.currentConnection();
    assertEquals(tag.id(), addPolicy(second.name(), catalog.name(), AUDIT_INFO));
    SessionUtils.rollbackTransaction();
    SessionUtils.beginTransaction();
    connection = SqlSessions.currentConnection();
    assertEquals(1, count("tag_meta"));
    assertEquals(1, count("policy_tag_relation_meta"));
    assertEquals(0, count("tag_relation_meta"));
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
                MetadataObjects.parse("catalog.schema.table.column", MetadataObject.Type.COLUMN),
                AUDIT_INFO));
    assertThrows(NullPointerException.class, () -> addPolicy(policy.name(), catalog.name(), null));
    assertEquals(0, count("tag_meta"));
    assertEquals(0, count("tag_relation_meta"));
  }

  /** Reserved-name collisions are rejected through the persisted tag model. */
  @TestTemplate
  public void testRejectsVisibleTagCollision() throws Exception {
    store.put(
        TagEntity.builder()
            .withId(100L)
            .withNamespace(policy.namespace())
            .withName(HiddenPolicyTag.name(policy.id()))
            .withAuditInfo(AUDIT_INFO)
            .build(),
        false);
    assertThrows(
        IllegalArgumentException.class, () -> addPolicy(policy.name(), catalog.name(), AUDIT_INFO));
    assertEquals(0, count("tag_relation_meta"));
  }

  /** Explicit audit metadata passes through relation operations and is preserved on retries. */
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
        assertEquals(JsonUtils.anyFieldMapper().writeValueAsString(audit), rows.getString(1));
      }
    }
  }

  /** A hidden tag cannot silently select another policy. */
  @TestTemplate
  public void testRejectsUnexpectedPolicyRelation() throws Exception {
    addPolicy(policy.name(), catalog.name(), AUDIT_INFO);
    PolicyEntity second = createPolicy(21L, policy.namespace(), "second_policy", AUDIT_INFO);
    store.put(second, false);
    store
        .relationOperations()
        .updateEntityRelations(
            RelationUpdate.of(
                SupportsRelationOperations.Type.POLICY_TAG_REL,
                NameIdentifierUtil.ofTag(METALAKE, HiddenPolicyTag.name(policy.id())),
                Entity.EntityType.TAG,
                new RelationEdgeTarget[] {
                  RelationEdgeTarget.of(second.nameIdentifier(), Entity.EntityType.POLICY, null)
                },
                null));
    assertThrows(
        IllegalArgumentException.class, () -> addPolicy(policy.name(), catalog.name(), AUDIT_INFO));
  }

  private long addPolicy(String name, String object, AuditInfo audit) throws Exception {
    return manager.addPolicyToObject(
        METALAKE, name, MetadataObjects.parse(object, MetadataObject.Type.CATALOG), audit);
  }

  private long count(String table) throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT COUNT(*) FROM " + table + " WHERE deleted_at = 0")) {
      rows.next();
      return rows.getLong(1);
    }
  }
}
