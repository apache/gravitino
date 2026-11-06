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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.utils.POConverters;

/** Internal object-policy binding operations backed by server-managed tags. */
final class PolicyObjectBindingService {

  private final Connection connection;

  /**
   * Creates a binding service using the caller's metadata transaction.
   *
   * @param connection The JDBC connection, owned by the caller.
   */
  PolicyObjectBindingService(Connection connection) {
    this.connection = connection;
  }

  /**
   * Applies a policy to one metadata object through its hidden tag, idempotently.
   *
   * <p>The tag is built with Gravitino's tag model when it does not exist. This method does not
   * commit, roll back, or close the connection; the caller owns the transaction.
   *
   * @param policyId The policy ID.
   * @param objectId The metadata object ID.
   * @param objectType The metadata object type.
   * @param auditInfo The association's serialized audit information.
   * @return The policy's hidden tag ID.
   * @throws SQLException If an endpoint is missing, a hidden tag is invalid, or storage fails.
   */
  long addPolicyToObject(
      long policyId, long objectId, MetadataObject.Type objectType, String auditInfo)
      throws SQLException {
    Preconditions.checkState(!connection.getAutoCommit(), "Policy binding requires a transaction");
    AuditInfo audit;
    try {
      audit = JsonUtils.anyFieldMapper().readValue(auditInfo, AuditInfo.class);
    } catch (JsonProcessingException e) {
      throw new SQLException("Invalid policy binding audit information", e);
    }
    if (audit == null) {
      throw new SQLException("Policy binding audit information is required");
    }
    long metalakeId;
    String metalake;
    try (PreparedStatement statement =
            prepare(
                "SELECT p.metalake_id, m.metalake_name FROM policy_meta p JOIN metalake_meta m"
                    + " ON p.metalake_id = m.metalake_id WHERE p.policy_id = ?"
                    + " AND p.deleted_at = 0 AND m.deleted_at = 0",
                policyId);
        ResultSet rows = statement.executeQuery()) {
      if (!rows.next()) {
        throw new SQLException("Policy or metalake is missing: " + policyId);
      }
      metalakeId = rows.getLong(1);
      metalake = rows.getString(2);
    }
    validateObject(metalakeId, objectId, objectType);
    long tagId = findHiddenTagId(policyId, metalakeId);
    if (tagId < 0) {
      TagEntity tag =
          HiddenPolicyTag.create(policyId, RandomIdGenerator.INSTANCE.nextId(), metalake, audit);
      TagPO row =
          POConverters.initializeTagPOWithVersion(tag, TagPO.builder().withMetalakeId(metalakeId));
      tagId = tag.id();
      execute(
          "INSERT INTO tag_meta (tag_id, tag_name, metalake_id, tag_comment, properties,"
              + " allowed_values, hidden, audit_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
          row.getTagId(),
          row.getTagName(),
          row.getMetalakeId(),
          row.getComment(),
          row.getProperties(),
          row.getAllowedValues(),
          row.isHidden(),
          row.getAuditInfo());
    }
    if (count(
            "SELECT COUNT(*) FROM policy_tag_relation_meta WHERE tag_id = ? AND deleted_at = 0"
                + " AND (policy_id <> ? OR selector IS NOT NULL)",
            tagId,
            policyId)
        != 0) {
      throw new SQLException("Invalid policy relation for hidden tag " + tagId);
    }
    if (count(
            "SELECT COUNT(*) FROM policy_tag_relation_meta WHERE tag_id = ? AND policy_id = ?"
                + " AND deleted_at = 0",
            tagId,
            policyId)
        == 0) {
      execute(
          "INSERT INTO policy_tag_relation_meta (policy_id, tag_id, audit_info) VALUES (?, ?, ?)",
          policyId,
          tagId,
          auditInfo);
    }
    if (count(
            "SELECT COUNT(*) FROM tag_relation_meta WHERE tag_id = ? AND metadata_object_id = ?"
                + " AND metadata_object_type = ? AND tag_value = '' AND deleted_at = 0",
            tagId,
            objectId,
            objectType.name())
        == 0) {
      execute(
          "INSERT INTO tag_relation_meta (tag_id, metadata_object_id, metadata_object_type,"
              + " tag_value, audit_info) VALUES (?, ?, ?, '', ?)",
          tagId,
          objectId,
          objectType.name(),
          auditInfo);
    }
    return tagId;
  }

  /**
   * Checks that a metadata object is active and belongs to the expected metalake.
   *
   * @param metalakeId The expected metalake ID.
   * @param objectId The metadata object ID.
   * @param objectType The metadata object type.
   * @throws SQLException If the type is unsupported or the object is missing or foreign.
   */
  void validateObject(long metalakeId, long objectId, MetadataObject.Type objectType)
      throws SQLException {
    switch (objectType) {
      case CATALOG:
      case SCHEMA:
      case TABLE:
      case VIEW:
      case FILESET:
      case TOPIC:
      case MODEL:
      case FUNCTION:
        String type = objectType.name().toLowerCase(Locale.ROOT);
        if (count(
                "SELECT COUNT(*) FROM "
                    + type
                    + "_meta WHERE "
                    + type
                    + "_id = ? AND metalake_id = ? AND deleted_at = 0",
                objectId,
                metalakeId)
            == 1) {
          return;
        }
        throw new SQLException(
            "Missing or foreign metadata object: " + objectType + ":" + objectId);
      default:
        throw new SQLException("Unsupported policy binding object type: " + objectType);
    }
  }

  /**
   * Looks up and validates the policy's hidden tag without modifying the database.
   *
   * @param policyId The policy ID.
   * @param metalakeId The policy's metalake ID.
   * @return The hidden tag ID, or -1 if no tag exists.
   * @throws SQLException If the reserved name belongs to an invalid tag or storage fails.
   */
  long findHiddenTagId(long policyId, long metalakeId) throws SQLException {
    try (PreparedStatement statement =
            prepare(
                "SELECT tag_id, properties, allowed_values, hidden FROM tag_meta"
                    + " WHERE metalake_id = ? AND tag_name = ? AND deleted_at = 0",
                metalakeId,
                HiddenPolicyTag.name(policyId));
        ResultSet rows = statement.executeQuery()) {
      if (!rows.next()) {
        return -1;
      }
      if (!HiddenPolicyTag.properties(policyId).equals(rows.getString(2))
          || !"[]".equals(rows.getString(3))
          || !rows.getBoolean(4)) {
        throw new SQLException("Reserved tag name collision for policy " + policyId);
      }
      return rows.getLong(1);
    }
  }

  private long count(String sql, Object... parameters) throws SQLException {
    try (PreparedStatement statement = prepare(sql, parameters);
        ResultSet rows = statement.executeQuery()) {
      rows.next();
      return rows.getLong(1);
    }
  }

  private void execute(String sql, Object... parameters) throws SQLException {
    try (PreparedStatement statement = prepare(sql, parameters)) {
      statement.executeUpdate();
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
}
