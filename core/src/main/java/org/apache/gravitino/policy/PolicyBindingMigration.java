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

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Offline conversion of legacy object-policy relations into server-managed tag relations. */
public final class PolicyBindingMigration {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyBindingMigration.class);
  private final Connection connection;
  private final PolicyObjectBindingService objectBindings;

  /**
   * Creates a migrator using an exclusively owned connection to an upgraded metadata database.
   *
   * @param connection A connection with auto-commit disabled. All servers must be stopped.
   */
  public PolicyBindingMigration(Connection connection) {
    this.connection = connection;
    this.objectBindings = new PolicyObjectBindingService(connection);
  }

  /**
   * Checks the upgrade prerequisites and reports pending work without modifying the database.
   *
   * @return The number of active legacy bindings.
   * @throws SQLException If the schema or a legacy binding is invalid.
   */
  public long dryRun() throws SQLException {
    requireTargetSchema();
    validateHiddenTags();
    if (!hasLegacyTable(connection)) {
      LOG.info("Precheck passed: no legacy policy binding table exists");
      return 0;
    }
    long count = 0;
    for (long policyId : pendingPolicies()) {
      long metalakeId = metalakeId(policyId);
      // Reject collisions before any policy is migrated.
      objectBindings.findHiddenTagId(policyId, metalakeId);
      for (Binding binding : bindings(policyId)) {
        objectBindings.validateObject(metalakeId, binding.objectId, objectType(binding.type));
        count++;
      }
    }
    LOG.info("Precheck passed: {} legacy policy bindings require migration", count);
    return count;
  }

  /**
   * Migrates each policy in one transaction and verifies the replacement before retiring old rows.
   *
   * <p>A failed policy is rolled back completely. Previously committed policies remain migrated;
   * rerunning the tool resumes with the remaining active legacy rows.
   *
   * @return The number of migrated legacy bindings.
   * @throws SQLException If validation, migration, or verification fails.
   */
  public long migrate() throws SQLException {
    Preconditions.checkState(!connection.getAutoCommit(), "Migration requires a transaction");
    dryRun();
    if (!hasLegacyTable(connection)) {
      return 0;
    }
    long migrated = 0;
    for (long policyId : pendingPolicies()) {
      try {
        List<Binding> bindings = bindings(policyId);
        long tagId = -1;
        for (Binding binding : bindings) {
          tagId =
              PolicyManager.addPolicyToObject(
                  connection,
                  policyId,
                  binding.objectId,
                  objectType(binding.type),
                  binding.auditInfo);
        }
        verifyReplacement(policyId, tagId, bindings);
        LOG.info("Verified {} replacement bindings for policy {}", bindings.size(), policyId);
        int cleaned =
            execute(
                "UPDATE policy_relation_meta SET deleted_at = ?, last_version = current_version + 1,"
                    + " current_version = current_version + 1 WHERE policy_id = ? AND deleted_at = 0",
                System.currentTimeMillis(),
                policyId);
        if (cleaned != bindings.size()) {
          throw new SQLException("Legacy bindings changed during migration; stop all servers");
        }
        connection.commit();
        migrated += cleaned;
        LOG.info("Committed policy {}: {} migrated and retired bindings", policyId, cleaned);
      } catch (SQLException | RuntimeException e) {
        connection.rollback();
        throw e;
      }
    }
    verify();
    return migrated;
  }

  /**
   * Verifies that no active legacy bindings remain and all hidden tags are valid.
   *
   * @throws SQLException If migration is incomplete or hidden bindings are invalid.
   */
  public void verify() throws SQLException {
    requireTargetSchema();
    checkMigrationComplete(connection);
    validateHiddenTags();
    LOG.info("Policy binding migration verification passed");
  }

  /**
   * Refuses server startup when active legacy bindings still need migration.
   *
   * @param connection The metadata database connection.
   * @throws SQLException If the migration is incomplete or its status cannot be read.
   */
  public static void checkMigrationComplete(Connection connection) throws SQLException {
    if (!hasLegacyTable(connection)) {
      return;
    }
    try (PreparedStatement statement =
            connection.prepareStatement(
                "SELECT id FROM policy_relation_meta WHERE deleted_at = 0");
        ResultSet rows = statement.executeQuery()) {
      if (rows.next()) {
        throw new SQLException(
            "Legacy policy bindings require offline migration. Stop all servers,"
                + " back up the database, and run bin/migrate-policy-bindings.sh before starting Gravitino.");
      }
    }
  }

  private static boolean hasLegacyTable(Connection connection) throws SQLException {
    // Match exact names in the current schema: '_' is a wildcard in JDBC metadata patterns.
    try (ResultSet tables =
        connection
            .getMetaData()
            .getTables(
                connection.getCatalog(), connection.getSchema(), "%", new String[] {"TABLE"})) {
      while (tables.next()) {
        if ("policy_relation_meta".equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
          return true;
        }
      }
    }
    return false;
  }

  private void requireTargetSchema() throws SQLException {
    // These reads also fail clearly if the 2.0 schema upgrade has not been applied.
    count("SELECT COUNT(allowed_values) FROM tag_meta WHERE 1 = 0");
    count("SELECT COUNT(hidden) FROM tag_meta WHERE 1 = 0");
    count("SELECT COUNT(tag_value) FROM tag_relation_meta WHERE 1 = 0");
    count("SELECT COUNT(selector) FROM policy_tag_relation_meta WHERE 1 = 0");
  }

  private List<Long> pendingPolicies() throws SQLException {
    List<Long> ids = new ArrayList<>();
    try (PreparedStatement statement =
            connection.prepareStatement(
                "SELECT DISTINCT policy_id FROM policy_relation_meta WHERE deleted_at = 0 ORDER BY policy_id");
        ResultSet rows = statement.executeQuery()) {
      while (rows.next()) {
        ids.add(rows.getLong(1));
      }
    }
    return ids;
  }

  private long metalakeId(long policyId) throws SQLException {
    try (PreparedStatement statement =
            prepare(
                "SELECT p.metalake_id FROM policy_meta p JOIN metalake_meta m"
                    + " ON p.metalake_id = m.metalake_id WHERE p.policy_id = ?"
                    + " AND p.deleted_at = 0 AND m.deleted_at = 0",
                policyId);
        ResultSet rows = statement.executeQuery()) {
      if (!rows.next()) {
        throw new SQLException("Legacy binding references missing policy or metalake: " + policyId);
      }
      return rows.getLong(1);
    }
  }

  private List<Binding> bindings(long policyId) throws SQLException {
    List<Binding> bindings = new ArrayList<>();
    try (PreparedStatement statement =
            prepare(
                "SELECT metadata_object_id, metadata_object_type, audit_info FROM policy_relation_meta"
                    + " WHERE policy_id = ? AND deleted_at = 0 ORDER BY id",
                policyId);
        ResultSet rows = statement.executeQuery()) {
      while (rows.next()) {
        bindings.add(new Binding(rows.getLong(1), rows.getString(2), rows.getString(3)));
      }
    }
    return bindings;
  }

  private void validateHiddenTags() throws SQLException {
    // Inspect names in Java so LIKE wildcard escaping cannot hide a reserved-name collision.
    try (PreparedStatement statement =
            connection.prepareStatement(
                "SELECT tag_id, tag_name, metalake_id FROM tag_meta WHERE deleted_at = 0");
        ResultSet rows = statement.executeQuery()) {
      while (rows.next()) {
        String name = rows.getString(2);
        if (!HiddenPolicyTag.isHidden(name)) {
          continue;
        }
        long policyId;
        try {
          policyId = HiddenPolicyTag.policyId(name);
        } catch (IllegalArgumentException e) {
          throw new SQLException("Invalid reserved tag name: " + name, e);
        }
        long tagId = rows.getLong(1);
        long metalakeId = rows.getLong(3);
        if (metalakeId(policyId) != metalakeId
            || objectBindings.findHiddenTagId(policyId, metalakeId) != tagId) {
          throw new SQLException("Invalid hidden tag identity: " + tagId);
        }
        if (count(
                "SELECT COUNT(*) FROM policy_tag_relation_meta WHERE tag_id = ?"
                    + " AND deleted_at = 0 AND (policy_id <> ? OR selector IS NOT NULL)",
                tagId,
                policyId)
            != 0) {
          throw new SQLException("Unexpected policy or selector for hidden tag " + tagId);
        }
        if (count(
                "SELECT COUNT(*) FROM policy_tag_relation_meta WHERE tag_id = ?"
                    + " AND policy_id = ? AND deleted_at = 0",
                tagId,
                policyId)
            != 1) {
          throw new SQLException("Missing policy relation for hidden tag " + tagId);
        }
        if (count(
                "SELECT COUNT(*) FROM tag_relation_meta WHERE tag_id = ?"
                    + " AND deleted_at = 0 AND tag_value <> ''",
                tagId)
            != 0) {
          throw new SQLException("Unexpected assignment value for hidden tag " + tagId);
        }
      }
    }
  }

  private void verifyReplacement(long policyId, long tagId, List<Binding> bindings)
      throws SQLException {
    if (count(
            "SELECT COUNT(*) FROM policy_tag_relation_meta WHERE tag_id = ?"
                + " AND deleted_at = 0 AND policy_id = ? AND selector IS NULL",
            tagId,
            policyId)
        != 1) {
      throw new SQLException("Replacement policy relation is missing for policy " + policyId);
    }
    for (Binding binding : bindings) {
      if (count(
              "SELECT COUNT(*) FROM tag_relation_meta WHERE tag_id = ?"
                  + " AND metadata_object_id = ? AND metadata_object_type = ?"
                  + " AND deleted_at = 0 AND tag_value = ''",
              tagId,
              binding.objectId,
              binding.type)
          != 1) {
        throw new SQLException("Replacement object relation is missing for policy " + policyId);
      }
    }
  }

  private MetadataObject.Type objectType(String type) throws SQLException {
    try {
      return MetadataObject.Type.valueOf(type);
    } catch (IllegalArgumentException e) {
      throw new SQLException("Unsupported or noncanonical legacy metadata object type: " + type, e);
    }
  }

  private long count(String sql, Object... parameters) throws SQLException {
    try (PreparedStatement statement = prepare(sql, parameters);
        ResultSet rows = statement.executeQuery()) {
      rows.next();
      return rows.getLong(1);
    }
  }

  private int execute(String sql, Object... parameters) throws SQLException {
    try (PreparedStatement statement = prepare(sql, parameters)) {
      return statement.executeUpdate();
    }
  }

  private PreparedStatement prepare(String sql, Object... parameters) throws SQLException {
    PreparedStatement statement = connection.prepareStatement(sql);
    try {
      for (int i = 0; i < parameters.length; i++) {
        statement.setObject(i + 1, parameters[i]);
      }
      return statement;
    } catch (SQLException | RuntimeException e) {
      statement.close();
      throw e;
    }
  }

  private static final class Binding {
    private final long objectId;
    private final String type;
    private final String auditInfo;

    private Binding(long objectId, String type, String auditInfo) {
      this.objectId = objectId;
      this.type = type;
      this.auditInfo = auditInfo;
    }
  }
}
