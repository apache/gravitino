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
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Exercises the offline migrator against every supported metadata database. */
public class TestPolicyBindingMigration extends TestJDBCBackend {

  private Connection connection;
  private PolicyEntity policy;
  private CatalogEntity catalog;

  /** Creates an upgraded database with one legacy binding. */
  @BeforeEach
  public void prepareLegacyDatabase() throws Exception {
    connection =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource()
            .getConnection();
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE policy_relation_meta (id BIGINT PRIMARY KEY, policy_id BIGINT NOT NULL,"
              + " metadata_object_id BIGINT NOT NULL, metadata_object_type VARCHAR(64) NOT NULL,"
              + " audit_info TEXT NOT NULL, current_version INT NOT NULL DEFAULT 1,"
              + " last_version INT NOT NULL DEFAULT 1, deleted_at BIGINT NOT NULL DEFAULT 0)");
    }
    connection.commit();
    createAndInsertMakeLake("migration_test");
    catalog = createAndInsertCatalog("migration_test", "catalog");
    policy = createPolicy(20L, Namespace.of("migration_test"), "policy", AUDIT_INFO);
    backend.insert(policy, false);
    removeAutomaticallyCreatedTag(policy.id());
    insertBinding(1, policy.id(), catalog.id(), "CATALOG");
    connection.commit();
  }

  /** Removes the legacy-only table, which is absent from new installations. */
  @AfterEach
  public void removeLegacyDatabase() throws SQLException {
    if (connection != null) {
      connection.rollback();
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS policy_relation_meta");
      }
      connection.commit();
      connection.close();
    }
  }

  /** Verifies no writes in precheck, startup gating, migration, auditing and idempotency. */
  @TestTemplate
  public void testMigrationAndVerification() throws Exception {
    PolicyBindingMigration migration = new PolicyBindingMigration(connection);
    assertEquals(1, migration.dryRun());
    assertEquals(0, count("tag_meta"));
    assertThrows(
        SQLException.class, () -> PolicyBindingMigration.checkMigrationComplete(connection));
    assertThrows(SQLException.class, migration::verify);
    assertEquals(1, migration.migrate());
    assertEquals(0, activeLegacyBindings());
    assertEquals(1, count("tag_meta"));
    assertEquals(1, count("tag_relation_meta"));
    assertEquals(1, count("policy_tag_relation_meta"));
    assertEquals(1, count("policy_relation_meta"));
    migration.verify();
    PolicyBindingMigration.checkMigrationComplete(connection);
    assertEquals(0, migration.migrate());
    assertEquals(1, count("tag_meta"));
    try (Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT r.audit_info, t.tag_name, t.allowed_values, p.selector, t.hidden FROM tag_relation_meta r"
                    + " JOIN tag_meta t ON r.tag_id = t.tag_id JOIN policy_tag_relation_meta p ON p.tag_id = t.tag_id")) {
      assertTrue(rows.next());
      assertEquals("{}", rows.getString(1));
      assertEquals(HiddenPolicyTag.name(policy.id()), rows.getString(2));
      assertEquals("[]", rows.getString(3));
      assertEquals(null, rows.getString(4));
      assertTrue(rows.getBoolean(5));
    }
  }

  /** Detects a missing policy-to-tag relation after migration. */
  @TestTemplate
  public void testVerifyDetectsMissingPolicyRelation() throws Exception {
    PolicyBindingMigration migration = new PolicyBindingMigration(connection);
    migration.migrate();
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("DELETE FROM policy_tag_relation_meta");
    }
    connection.commit();
    assertThrows(SQLException.class, migration::verify);
  }

  /** Verifies a failed cleanup rolls back only its policy and a retry resumes safely. */
  @TestTemplate
  public void testInterruptedMigrationResumesAfterCommittedPolicy() throws Exception {
    PolicyEntity second = createPolicy(21L, policy.namespace(), "second_policy", AUDIT_INFO);
    backend.insert(second, false);
    removeAutomaticallyCreatedTag(second.id());
    insertBinding(2, second.id(), catalog.id(), "CATALOG");
    connection.commit();
    Connection failing = spy(connection);
    AtomicInteger cleanups = new AtomicInteger();
    doAnswer(
            invocation -> {
              if (cleanups.incrementAndGet() == 2) {
                throw new SQLException("Injected cleanup failure");
              }
              return connection.prepareStatement(invocation.getArgument(0));
            })
        .when(failing)
        .prepareStatement(startsWith("UPDATE policy_relation_meta"));
    assertThrows(SQLException.class, () -> new PolicyBindingMigration(failing).migrate());
    assertEquals(1, activeLegacyBindings());
    assertEquals(1, count("tag_meta"));
    assertEquals(1, count("tag_relation_meta"));
    assertEquals(1, new PolicyBindingMigration(connection).migrate());
    assertEquals(2, count("tag_meta"));
    new PolicyBindingMigration(connection).verify();
  }

  /** Refuses a collision before changing any legacy row. */
  @TestTemplate
  public void testReservedNameCollision() throws Exception {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO tag_meta (tag_id, tag_name, metalake_id, audit_info)"
                + " SELECT 100, ?, metalake_id, '{}' FROM policy_meta WHERE policy_id = ?")) {
      statement.setString(1, HiddenPolicyTag.name(policy.id()));
      statement.setLong(2, policy.id());
      statement.executeUpdate();
    }
    connection.commit();
    assertThrows(SQLException.class, () -> new PolicyBindingMigration(connection).dryRun());
    assertThrows(SQLException.class, () -> new PolicyBindingMigration(connection).migrate());
    assertEquals(1, activeLegacyBindings());
    assertEquals(0, count("tag_relation_meta"));
  }

  /** Rejects dangling references in precheck rather than losing bindings during conversion. */
  @TestTemplate
  public void testMissingObjectBlocksAllWrites() throws Exception {
    insertBinding(2, policy.id(), Long.MAX_VALUE, "TABLE");
    connection.commit();
    assertThrows(SQLException.class, () -> new PolicyBindingMigration(connection).dryRun());
    assertThrows(SQLException.class, () -> new PolicyBindingMigration(connection).migrate());
    assertEquals(0, count("tag_meta"));
    assertEquals(2, activeLegacyBindings());
  }

  /** Rejects references to a live object in another metalake. */
  @TestTemplate
  public void testForeignObjectBlocksMigration() throws Exception {
    createAndInsertMakeLake("other_metalake");
    CatalogEntity foreign = createAndInsertCatalog("other_metalake", "foreign_catalog");
    insertBinding(2, policy.id(), foreign.id(), "CATALOG");
    connection.commit();
    assertThrows(SQLException.class, () -> new PolicyBindingMigration(connection).migrate());
    assertEquals(0, count("tag_meta"));
  }

  /** Rejects missing policies before committing any replacement. */
  @TestTemplate
  public void testMissingPolicyBlocksMigration() throws Exception {
    insertBinding(2, Long.MAX_VALUE, catalog.id(), "CATALOG");
    connection.commit();
    assertThrows(SQLException.class, () -> new PolicyBindingMigration(connection).migrate());
    assertEquals(0, count("tag_meta"));
  }

  /** Direct column and metalake bindings were never supported by the historical model. */
  @TestTemplate
  public void testUnsupportedLegacyObjectType() throws Exception {
    for (String type : new String[] {"COLUMN", "catalog"}) {
      insertBinding(2, policy.id(), catalog.id(), type);
      connection.commit();
      assertThrows(SQLException.class, () -> new PolicyBindingMigration(connection).dryRun());
      assertEquals(0, count("tag_meta"));
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate("DELETE FROM policy_relation_meta WHERE id = 2");
      }
      connection.commit();
    }
  }

  /** Hidden tags must remain value-less after migration. */
  @TestTemplate
  public void testVerifyRejectsAssignmentValues() throws Exception {
    PolicyBindingMigration migration = new PolicyBindingMigration(connection);
    migration.migrate();
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "UPDATE tag_relation_meta SET tag_value = 'unexpected' WHERE deleted_at = 0");
    }
    connection.commit();
    assertThrows(SQLException.class, migration::verify);
  }

  /** A changed selector must not silently narrow a migrated binding. */
  @TestTemplate
  public void testVerifyRejectsChangedSelector() throws Exception {
    PolicyBindingMigration migration = new PolicyBindingMigration(connection);
    migration.migrate();
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "UPDATE policy_tag_relation_meta SET selector = '{}' WHERE deleted_at = 0");
    }
    connection.commit();
    assertThrows(SQLException.class, migration::verify);
  }

  /** Does not migrate already deleted legacy bindings. */
  @TestTemplate
  public void testDeletedBindingsAreIgnored() throws Exception {
    insertBinding(2, Long.MAX_VALUE, Long.MAX_VALUE, "TABLE");
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("UPDATE policy_relation_meta SET deleted_at = 1 WHERE id = 2");
    }
    connection.commit();
    assertEquals(1, new PolicyBindingMigration(connection).migrate());
    assertEquals(1, count("tag_relation_meta"));
  }

  /** Ensures policy deletion cleans the hidden tag and assignments in its storage transaction. */
  @TestTemplate
  public void testPolicyDeletionCleansHiddenTag() throws Exception {
    new PolicyBindingMigration(connection).migrate();
    connection.rollback();
    assertTrue(backend.delete(policy.nameIdentifier(), Entity.EntityType.POLICY, false));
    connection.rollback();
    assertEquals(0, activeCount("tag_meta"));
    assertEquals(0, activeCount("tag_relation_meta"));
    assertEquals(0, activeCount("policy_tag_relation_meta"));
    new PolicyBindingMigration(connection).verify();
  }

  /** Fresh installations have no legacy table and do not require a migration. */
  @TestTemplate
  public void testFreshDatabase() throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute("DROP TABLE policy_relation_meta");
    }
    connection.commit();
    PolicyBindingMigration migration = new PolicyBindingMigration(connection);
    assertEquals(0, migration.dryRun());
    assertEquals(0, migration.migrate());
    migration.verify();
    PolicyBindingMigration.checkMigrationComplete(connection);
  }

  private void removeAutomaticallyCreatedTag(long policyId) throws SQLException {
    // This fixture represents a policy persisted by a pre-hidden-tag server.
    try (PreparedStatement relation =
            connection.prepareStatement(
                "DELETE FROM policy_tag_relation_meta WHERE policy_id = ?");
        PreparedStatement tag =
            connection.prepareStatement("DELETE FROM tag_meta WHERE tag_name = ?")) {
      relation.setLong(1, policyId);
      relation.executeUpdate();
      tag.setString(1, HiddenPolicyTag.name(policyId));
      tag.executeUpdate();
    }
  }

  private void insertBinding(long id, long policyId, long objectId, String type)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO policy_relation_meta (id, policy_id, metadata_object_id, metadata_object_type, audit_info)"
                + " VALUES (?, ?, ?, ?, '{}')")) {
      statement.setLong(1, id);
      statement.setLong(2, policyId);
      statement.setLong(3, objectId);
      statement.setString(4, type);
      statement.executeUpdate();
    }
  }

  private long activeLegacyBindings() throws SQLException {
    return activeCount("policy_relation_meta");
  }

  private long activeCount(String table) throws SQLException {
    return queryCount("SELECT COUNT(*) FROM " + table + " WHERE deleted_at = 0");
  }

  private long count(String table) throws SQLException {
    return queryCount("SELECT COUNT(*) FROM " + table);
  }

  private long queryCount(String sql) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery(sql)) {
      rows.next();
      return rows.getLong(1);
    }
  }
}
