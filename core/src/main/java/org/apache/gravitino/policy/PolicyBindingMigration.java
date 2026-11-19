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

import static org.apache.gravitino.tag.TagManager.SUPPORTED_METADATA_OBJECT_TYPES_FOR_TAGS;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.service.MetadataObjectService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.tag.TagValueConstraint;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Offline conversion of legacy object-policy relations into server-managed tag relations. */
public final class PolicyBindingMigration {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyBindingMigration.class);
  private final Connection connection;
  private final EntityStore entityStore;
  private final PolicyManager policyManager;

  /**
   * Creates a migrator in the current SQL session. All servers must be stopped before running it.
   *
   * @param connection The current entity store session's connection.
   * @param entityStore An uncached entity store participating in that session.
   */
  public PolicyBindingMigration(Connection connection, EntityStore entityStore) {
    this.connection = connection;
    this.entityStore = entityStore;
    this.policyManager = new PolicyManager(RandomIdGenerator.INSTANCE, entityStore);
  }

  /**
   * Converts and verifies each policy's bindings before retiring its legacy rows in one
   * transaction. A failure rolls back that policy and is reported after the remaining policies are
   * processed. If rollback fails, processing stops and the summary includes unprocessed policies.
   * Rerunning resumes from the active legacy rows. Missing owners and objects are logged and
   * skipped without retiring their bindings.
   *
   * @return The number of migrated bindings.
   * @throws SQLException If a binding cannot be converted or verified.
   */
  public long migrate() throws SQLException {
    Preconditions.checkState(!connection.getAutoCommit(), "Migration requires a transaction");
    long migrated = 0;
    long skippedPolicies = 0;
    long skippedBindings = 0;
    long skippedObjectBindings = 0;
    long failedPolicies = 0;
    SQLException firstFailure = null;
    List<Long> policyIds = pendingPolicies();
    long unprocessedPolicies = policyIds.size();
    for (long policyId : policyIds) {
      unprocessedPolicies--;
      try {
        long bindings =
            count(
                "SELECT COUNT(*) FROM policy_relation_meta WHERE policy_id = ? AND deleted_at = 0",
                policyId);
        PolicyEntity policy = loadPolicy(policyId);
        if (policy == null) {
          skippedPolicies++;
          skippedBindings += bindings;
          LOG.warn(
              "Skipping {} legacy bindings for policy_id={}: policy or metalake is missing or deleted;"
                  + " bindings remain active",
              bindings,
              policyId);
          continue;
        }
        TagEntity tag = hiddenTag(policy);
        long converted = bindObjects(policy);
        verifyReplacement(policyId, tag.id(), converted);
        try (PreparedStatement statement =
            prepare(
                "UPDATE policy_relation_meta SET deleted_at = ?, last_version = current_version + 1,"
                    + " current_version = current_version + 1 WHERE policy_id = ? AND deleted_at = 0"
                    + " AND EXISTS (SELECT 1 FROM tag_relation_meta r WHERE r.tag_id = ?"
                    + " AND r.metadata_object_id = policy_relation_meta.metadata_object_id"
                    + " AND r.metadata_object_type = policy_relation_meta.metadata_object_type"
                    + " AND r.deleted_at = 0 AND r.tag_value = '')",
                System.currentTimeMillis(),
                policyId,
                tag.id())) {
          if (statement.executeUpdate() != converted) {
            throw new SQLException("Legacy bindings changed for policy " + policyId);
          }
        }
        connection.commit();
        migrated += converted;
        skippedObjectBindings += bindings - converted;
        LOG.info("Migrated {} bindings for policy {}", converted, policyId);
      } catch (IOException | SQLException | RuntimeException e) {
        failedPolicies++;
        LOG.error("Failed to migrate policy_id={}; legacy bindings remain active", policyId, e);
        if (firstFailure == null) {
          firstFailure =
              new SQLException(
                  "Cannot migrate policy " + policyId + "; legacy bindings remain active", e);
        }
        try {
          connection.rollback();
        } catch (SQLException rollbackFailure) {
          firstFailure.addSuppressed(rollbackFailure);
          LOG.error(
              "Rollback failed for policy_id={}; stopping migration with {} unprocessed policies",
              policyId,
              unprocessedPolicies,
              rollbackFailure);
          break;
        }
      } finally {
        // Balance this temporary reference; the caller still owns the thread-bound session.
        SqlSessions.getSqlSession().clearCache();
        SqlSessions.closeSqlSession();
      }
    }
    LOG.info(
        "Migration complete: migrated_bindings={}, skipped_orphan_policies={}, skipped_orphan_bindings={},"
            + " skipped_missing_object_bindings={}, failed_policies={}, unprocessed_policies={};"
            + " skipped bindings remain active",
        migrated,
        skippedPolicies,
        skippedBindings,
        skippedObjectBindings,
        failedPolicies,
        unprocessedPolicies);
    if (skippedBindings > 0 || skippedObjectBindings > 0) {
      LOG.warn(
          "Migration left {} orphaned bindings and {} bindings to missing objects active in the legacy"
              + " table. The new server does not read these bindings; review the logged IDs before"
              + " completing the upgrade.",
          skippedBindings,
          skippedObjectBindings);
    }
    if (firstFailure != null) {
      throw firstFailure;
    }
    return migrated;
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

  @Nullable
  private PolicyEntity loadPolicy(long policyId) throws IOException, SQLException {
    try (PreparedStatement statement =
            prepare(
                "SELECT m.metalake_name, p.policy_name FROM policy_meta p JOIN metalake_meta m"
                    + " ON p.metalake_id = m.metalake_id WHERE p.policy_id = ?"
                    + " AND p.deleted_at = 0 AND m.deleted_at = 0",
                policyId);
        ResultSet rows = statement.executeQuery()) {
      if (!rows.next()) {
        return null;
      }
      return entityStore.get(
          NameIdentifierUtil.ofPolicy(rows.getString(1), rows.getString(2)),
          Entity.EntityType.POLICY,
          PolicyEntity.class);
    }
  }

  private TagEntity hiddenTag(PolicyEntity policy) throws IOException, SQLException {
    NameIdentifier identifier =
        NameIdentifierUtil.ofTag(policy.namespace().level(0), HiddenPolicyTag.name(policy.id()));
    try {
      TagEntity tag = entityStore.get(identifier, Entity.EntityType.TAG, TagEntity.class);
      if (!HiddenPolicyTag.create(policy, tag.id()).properties().equals(tag.properties())
          || tag.valueConstraint().type() != TagValueConstraint.Type.NO_VALUE) {
        throw new SQLException(
            "Hidden tag name collision for policy " + policy.id() + ": " + identifier);
      }
      return tag;
    } catch (NoSuchEntityException e) {
      TagEntity tag = HiddenPolicyTag.create(policy, RandomIdGenerator.INSTANCE.nextId());
      entityStore.put(tag, false);
      entityStore
          .relationOperations()
          .updateEntityRelations(
              RelationUpdate.of(
                      SupportsRelationOperations.Type.POLICY_TAG_REL,
                      identifier,
                      Entity.EntityType.TAG,
                      new RelationEdgeTarget[] {
                        RelationEdgeTarget.of(
                            policy.nameIdentifier(), Entity.EntityType.POLICY, null)
                      },
                      null)
                  .withAuditInfo(policy.auditInfo()));
      return tag;
    }
  }

  private long bindObjects(PolicyEntity policy) throws IOException, SQLException {
    long converted = 0;
    try (PreparedStatement statement =
            prepare(
                "SELECT metadata_object_id, metadata_object_type, audit_info FROM policy_relation_meta"
                    + " WHERE policy_id = ? AND deleted_at = 0 ORDER BY id",
                policy.id());
        ResultSet rows = statement.executeQuery()) {
      while (rows.next()) {
        long objectId = rows.getLong(1);
        MetadataObject.Type type = MetadataObject.Type.valueOf(rows.getString(2));
        Preconditions.checkArgument(
            SUPPORTED_METADATA_OBJECT_TYPES_FOR_TAGS.contains(type),
            "Unsupported policy binding object type: %s",
            type);
        List<MetadataObject> objects =
            MetadataObjectService.fromGenericEntities(
                Collections.singletonList(
                    GenericEntity.builder()
                        .withId(objectId)
                        .withEntityType(MetadataObjectUtil.toEntityType(type))
                        .build()));
        if (objects.isEmpty()) {
          LOG.warn(
              "Skipping legacy binding for policy_id={}, metadata_object_id={}, metadata_object_type={}:"
                  + " target is missing or deleted; binding remains active",
              policy.id(),
              objectId,
              type);
          continue;
        }
        policyManager.addPolicyToObject(
            policy.namespace().level(0),
            policy.name(),
            objects.get(0),
            JsonUtils.anyFieldMapper().readValue(rows.getString(3), AuditInfo.class));
        converted++;
      }
    }
    return converted;
  }

  private void verifyReplacement(long policyId, long tagId, long converted) throws SQLException {
    // addPolicyToObject does not check the policy-to-tag relation. Verify it before retiring rows.
    if (count(
                "SELECT COUNT(*) FROM policy_tag_relation_meta WHERE tag_id = ? AND deleted_at = 0",
                tagId)
            != 1
        || count(
                "SELECT COUNT(*) FROM policy_tag_relation_meta WHERE tag_id = ?"
                    + " AND policy_id = ? AND selector IS NULL AND deleted_at = 0",
                tagId,
                policyId)
            != 1) {
      throw new SQLException(
          "Replacement policy relation is missing or invalid for policy " + policyId);
    }
    if (count(
            "SELECT COUNT(*) FROM policy_relation_meta l WHERE l.policy_id = ?"
                + " AND l.deleted_at = 0 AND EXISTS (SELECT 1 FROM tag_relation_meta r"
                + " WHERE r.tag_id = ? AND r.metadata_object_id = l.metadata_object_id"
                + " AND r.metadata_object_type = l.metadata_object_type"
                + " AND r.deleted_at = 0 AND r.tag_value = '')",
            policyId,
            tagId)
        != converted) {
      throw new SQLException("Replacement object relation is missing for policy " + policyId);
    }
  }

  private long count(String sql, Object... parameters) throws SQLException {
    try (PreparedStatement statement = prepare(sql, parameters);
        ResultSet rows = statement.executeQuery()) {
      rows.next();
      return rows.getLong(1);
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
