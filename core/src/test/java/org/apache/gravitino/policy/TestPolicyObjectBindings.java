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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.TagMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Tests internal policy object bindings using caller-owned metadata transactions. */
public class TestPolicyObjectBindings extends TestJDBCBackend {

  private Connection connection;
  private PolicyEntity policy;
  private CatalogEntity catalog;

  /** Creates a policy with no hidden tag to represent an existing policy. */
  @BeforeEach
  public void preparePolicy() throws Exception {
    createAndInsertMakeLake("migration_test");
    catalog = createAndInsertCatalog("migration_test", "catalog");
    policy = createPolicy(20L, Namespace.of("migration_test"), "policy", AUDIT_INFO);
    backend.insert(policy, false);
    connection =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource()
            .getConnection();
    try (PreparedStatement relation =
            connection.prepareStatement(
                "DELETE FROM policy_tag_relation_meta WHERE policy_id = ?");
        PreparedStatement tag =
            connection.prepareStatement("DELETE FROM tag_meta WHERE tag_name = ?")) {
      relation.setLong(1, policy.id());
      relation.executeUpdate();
      tag.setString(1, HiddenPolicyTag.name(policy.id()));
      tag.executeUpdate();
    }
    connection.commit();
  }

  /** Releases the caller-owned transaction after each test. */
  @AfterEach
  public void closeConnection() throws SQLException {
    if (connection != null) {
      connection.rollback();
      connection.close();
    }
  }

  /** Internal policy application creates both links idempotently without committing the caller. */
  @TestTemplate
  public void testAddPolicyToObjectIsIdempotentAndCallerTransactional() throws Exception {
    long tagId =
        PolicyManager.addPolicyToObject(
            connection, policy.id(), catalog.id(), MetadataObject.Type.CATALOG, "{}");
    assertEquals(
        tagId,
        PolicyManager.addPolicyToObject(
            connection, policy.id(), catalog.id(), MetadataObject.Type.CATALOG, "{}"));
    assertEquals(1, count("tag_meta"));
    assertEquals(1, count("policy_tag_relation_meta"));
    assertEquals(1, count("tag_relation_meta"));
    connection.rollback();
    assertEquals(0, count("tag_meta"));
    assertEquals(0, count("policy_tag_relation_meta"));
    assertEquals(0, count("tag_relation_meta"));
  }

  /** Existing policy-created hidden tags are reused rather than replaced. */
  @TestTemplate
  public void testAddPolicyToObjectReusesPolicyTag() throws Exception {
    PolicyEntity second = createPolicy(21L, policy.namespace(), "second_policy", AUDIT_INFO);
    backend.insert(second, false);
    TagEntity tag =
        TagMetaService.getInstance()
            .getTagByIdentifier(
                NameIdentifierUtil.ofTag("migration_test", HiddenPolicyTag.name(second.id())));
    assertEquals(
        tag.id(),
        PolicyManager.addPolicyToObject(
            connection, second.id(), catalog.id(), MetadataObject.Type.CATALOG, "{}"));
    assertEquals(1, count("tag_meta"));
    assertEquals(1, count("policy_tag_relation_meta"));
    assertEquals(1, count("tag_relation_meta"));
    connection.rollback();
    assertEquals(1, count("tag_meta"));
    assertEquals(1, count("policy_tag_relation_meta"));
    assertEquals(0, count("tag_relation_meta"));
  }

  /** The internal entry point rejects invalid endpoints before writing a tag or association. */
  @TestTemplate
  public void testAddPolicyToObjectRejectsInvalidEndpoints() throws Exception {
    assertThrows(
        SQLException.class,
        () ->
            PolicyManager.addPolicyToObject(
                connection, Long.MAX_VALUE, catalog.id(), MetadataObject.Type.CATALOG, "{}"));
    assertThrows(
        SQLException.class,
        () ->
            PolicyManager.addPolicyToObject(
                connection, policy.id(), Long.MAX_VALUE, MetadataObject.Type.CATALOG, "{}"));
    assertThrows(
        SQLException.class,
        () ->
            PolicyManager.addPolicyToObject(
                connection, policy.id(), catalog.id(), MetadataObject.Type.COLUMN, "{}"));
    createAndInsertMakeLake("other_metalake");
    CatalogEntity foreign = createAndInsertCatalog("other_metalake", "foreign_catalog");
    assertThrows(
        SQLException.class,
        () ->
            PolicyManager.addPolicyToObject(
                connection, policy.id(), foreign.id(), MetadataObject.Type.CATALOG, "{}"));
    assertEquals(0, count("tag_meta"));
    assertEquals(0, count("tag_relation_meta"));
  }

  /** Invalid audit data and auto-commit connections are rejected without writing. */
  @TestTemplate
  public void testRejectsInvalidTransactionAndAudit() throws Exception {
    assertThrows(
        SQLException.class,
        () ->
            PolicyManager.addPolicyToObject(
                connection,
                policy.id(),
                catalog.id(),
                MetadataObject.Type.CATALOG,
                "invalid-json"));
    connection.setAutoCommit(true);
    assertThrows(
        IllegalStateException.class,
        () ->
            PolicyManager.addPolicyToObject(
                connection, policy.id(), catalog.id(), MetadataObject.Type.CATALOG, "{}"));
    connection.setAutoCommit(false);
    assertEquals(0, count("tag_meta"));
    assertEquals(0, count("policy_tag_relation_meta"));
    assertEquals(0, count("tag_relation_meta"));
  }

  private long count(String table) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
      rows.next();
      return rows.getLong(1);
    }
  }
}
