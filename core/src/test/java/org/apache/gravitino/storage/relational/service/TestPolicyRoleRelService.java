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
package org.apache.gravitino.storage.relational.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.PolicyRoleRelMapper;
import org.apache.gravitino.storage.relational.po.PolicyRoleRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/** Tests the policy-to-role references derived from access control policy content. */
public class TestPolicyRoleRelService extends TestJDBCBackend {

  private static final String METALAKE = "policy_role_relation_metalake";

  @TestTemplate
  public void testReferencesFollowPolicyContent() throws IOException {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy = accessControlPolicy("reader", Arrays.asList("analyst", "auditor"));
    backend.insert(policy, false);

    Assertions.assertEquals(Arrays.asList("analyst", "auditor"), rolesOf(policy.id()));
    Assertions.assertEquals(Collections.singletonList("reader"), policiesReferencing("analyst"));
    Long auditorRowId = referenceTo(policy.id(), "auditor").getId();

    backend.update(
        policy.nameIdentifier(),
        Entity.EntityType.POLICY,
        entity -> withRoles(policy, Arrays.asList("auditor", "steward")));

    Assertions.assertEquals(Arrays.asList("auditor", "steward"), rolesOf(policy.id()));
    Assertions.assertEquals(Collections.emptyList(), policiesReferencing("analyst"));
    // A reference the content still names is left alone rather than rewritten.
    Assertions.assertEquals(auditorRowId, referenceTo(policy.id(), "auditor").getId());
  }

  @TestTemplate
  public void testOverwriteAttachesReferencesToTheSurvivingPolicyRow() throws IOException {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity original = accessControlPolicy("reader", Collections.singletonList("analyst"));
    backend.insert(original, false);

    PolicyEntity replacement = accessControlPolicy("reader", Collections.singletonList("auditor"));
    backend.insert(replacement, true);

    // An overwrite keeps the row the name already pointed at, so the references follow that ID.
    Assertions.assertEquals(Collections.singletonList("auditor"), rolesOf(original.id()));
    Assertions.assertEquals(Collections.emptyList(), rolesOf(replacement.id()));
    Assertions.assertEquals(Collections.singletonList("reader"), policiesReferencing("auditor"));
    Assertions.assertEquals(Collections.emptyList(), policiesReferencing("analyst"));
  }

  @TestTemplate
  public void testFailedPolicyWriteLeavesNoReferences() throws IOException {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy = accessControlPolicy("reader", Collections.singletonList("analyst"));
    backend.insert(policy, false);

    PolicyEntity duplicate = accessControlPolicy("reader", Collections.singletonList("steward"));
    Assertions.assertThrows(
        EntityAlreadyExistsException.class, () -> backend.insert(duplicate, false));

    Assertions.assertEquals(Collections.emptyList(), policiesReferencing("steward"));
    Assertions.assertEquals(Collections.singletonList("reader"), policiesReferencing("analyst"));
  }

  @TestTemplate
  public void testFailedReferenceWriteRollsBackThePolicy() throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    String oversizedRoleName = "r".repeat(129);
    PolicyEntity policy =
        accessControlPolicy("reader", Collections.singletonList(oversizedRoleName));

    Assertions.assertThrows(RuntimeException.class, () -> backend.insert(policy, false));

    Assertions.assertEquals(
        0L, count("SELECT COUNT(*) FROM policy_meta WHERE policy_id = ?", policy.id()));
    Assertions.assertEquals(
        0L, count("SELECT COUNT(*) FROM policy_version_info WHERE policy_id = ?", policy.id()));
    Assertions.assertEquals(
        0L,
        count("SELECT COUNT(*) FROM policy_role_relation_meta WHERE policy_id = ?", policy.id()));
  }

  @TestTemplate
  public void testDuplicateRoleNamesCollapseToOneReference() throws IOException {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy = accessControlPolicy("reader", Arrays.asList("analyst", "analyst"));
    backend.insert(policy, false);

    Assertions.assertEquals(Collections.singletonList("analyst"), rolesOf(policy.id()));
  }

  @TestTemplate
  public void testReferenceEqualityFollowsTheRoleTable() throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE, "policy_role_equality_catalog");

    // Ask the authoritative table whether these two names are one role or two, rather than
    // assuming: MySQL compares role names ignoring trailing spaces, PostgreSQL and H2 do not.
    backend.insert(role(catalog, "analyst"), false);
    boolean roleTableSeesTwoRoles;
    try {
      backend.insert(role(catalog, "analyst "), false);
      roleTableSeesTwoRoles = true;
    } catch (EntityAlreadyExistsException sameRoleTwice) {
      roleTableSeesTwoRoles = false;
    }

    PolicyEntity policy = accessControlPolicy("reader", Arrays.asList("analyst", "analyst "));
    if (roleTableSeesTwoRoles) {
      backend.insert(policy, false);
      Assertions.assertEquals(Arrays.asList("analyst", "analyst "), rolesOf(policy.id()));
    } else {
      // One role named twice. The write fails against the role that collided, not the policy name.
      IllegalArgumentException failure =
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> backend.insert(policy, false));
      Assertions.assertTrue(failure.getMessage().contains("analyst"), failure.getMessage());
      Assertions.assertEquals(0L, activeReferenceCount(policy.id()));
    }
  }

  @TestTemplate
  public void testPolicyWithoutAccessControlContentReferencesNoRoles() throws IOException {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE),
            "compaction",
            AUDIT_INFO);
    backend.insert(policy, false);

    Assertions.assertEquals(Collections.emptyList(), rolesOf(policy.id()));
  }

  @TestTemplate
  public void testPolicyDeleteClearsReferences() throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy = accessControlPolicy("reader", Collections.singletonList("analyst"));
    backend.insert(policy, false);

    Assertions.assertTrue(backend.delete(policy.nameIdentifier(), Entity.EntityType.POLICY, false));

    Assertions.assertEquals(0L, activeReferenceCount(policy.id()));
  }

  @TestTemplate
  public void testMetalakeDeleteClearsOnlyItsOwnReferences() throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    String otherMetalake = METALAKE + "_other";
    createAndInsertMakeLake(otherMetalake);

    PolicyEntity policy = accessControlPolicy("reader", Collections.singletonList("analyst"));
    backend.insert(policy, false);
    PolicyEntity otherPolicy =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withNamespace(NamespaceUtil.ofPolicy(otherMetalake))
            .withName("reader")
            .withPolicyType(Policy.BuiltInType.ACCESS_CONTROL)
            .withComment("")
            .withEnabled(true)
            .withContent(
                PolicyContents.accessControl(
                    Collections.singletonList(Privilege.Name.SELECT_TABLE),
                    Collections.singletonList("analyst")))
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(otherPolicy, false);

    Assertions.assertTrue(
        backend.delete(NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE, true));

    Assertions.assertEquals(0L, activeReferenceCount(policy.id()));
    Assertions.assertEquals(1L, activeReferenceCount(otherPolicy.id()));
  }

  @TestTemplate
  public void testMetalakeDeleteClearsReferencesOfAnInFlightPolicyUpdate() throws Exception {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy = accessControlPolicy("reader", Collections.singletonList("analyst"));
    backend.insert(policy, false);

    AtomicReference<Throwable> deleteFailure = new AtomicReference<>();
    Thread deleter =
        new Thread(
            () -> {
              try {
                backend.delete(NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE, true);
              } catch (Throwable t) {
                deleteFailure.set(t);
              }
            });

    // Stand in for a policy update that holds the policy row and adds a reference mid-delete.
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(false);
        Connection updater = sqlSession.getConnection()) {
      updater.setAutoCommit(false);
      updater.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      try (PreparedStatement lock =
          updater.prepareStatement(
              "SELECT policy_id FROM policy_meta WHERE policy_id = ? FOR UPDATE")) {
        lock.setLong(1, policy.id());
        lock.executeQuery().close();
      }

      deleter.start();
      // Only the policy lives in this metalake, so the delete reaches its row almost at once and
      // the lock above is the one thing that can hold it up. Adding other entities here would
      // slow that down and make this wait too short.
      Thread.sleep(400);
      Assertions.assertTrue(deleter.isAlive(), "The metalake delete should not have finished yet");

      try (PreparedStatement insert =
          updater.prepareStatement(
              "INSERT INTO policy_role_relation_meta"
                  + " (policy_id, role_name, current_version, last_version, deleted_at)"
                  + " VALUES (?, 'auditor', 1, 1, 0)")) {
        insert.setLong(1, policy.id());
        insert.executeUpdate();
      }
      updater.commit();
    }

    deleter.join(30_000);
    Assertions.assertFalse(deleter.isAlive());
    Assertions.assertNull(deleteFailure.get());
    Assertions.assertEquals(0L, activeReferenceCount(policy.id()));
  }

  @TestTemplate
  public void testNonCascadeMetalakeDeleteClearsReferencesOfAnInFlightPolicyUpdate()
      throws Exception {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy = accessControlPolicy("reader", Collections.singletonList("analyst"));
    backend.insert(policy, false);

    AtomicReference<Throwable> deleteFailure = new AtomicReference<>();

    // The updater callback runs before the write transaction opens, so finishing the delete here
    // stands in for an update that read the policy before the metalake went away and commits
    // after. The delete soft-deletes the policy row before the references, so this update can no
    // longer write one: its own policy row is gone by the time it reaches the reference writes.
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            backend.update(
                policy.nameIdentifier(),
                Entity.EntityType.POLICY,
                entity -> {
                  deleteMetalakeWithoutCascade(deleteFailure);
                  return withRoles(policy, Arrays.asList("analyst", "auditor"));
                }));

    Assertions.assertNull(deleteFailure.get());
    Assertions.assertEquals(0L, activeReferenceCount(policy.id()));
  }

  @TestTemplate
  public void testSupersededReferencesLeaveNoTombstone() throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy = accessControlPolicy("reader", Arrays.asList("analyst", "auditor"));
    backend.insert(policy, false);

    // Remove a role, name it again, and remove it once more. Each removal takes the row out
    // rather than stamping it, so neither leaves a tombstone the next one could collide with,
    // whatever the database clock reads.
    for (List<String> roles :
        Arrays.asList(
            Collections.singletonList("auditor"),
            Arrays.asList("analyst", "auditor"),
            Collections.singletonList("auditor"))) {
      backend.update(
          policy.nameIdentifier(), Entity.EntityType.POLICY, entity -> withRoles(policy, roles));
      Assertions.assertEquals(0L, softDeletedReferenceCount());
    }

    Assertions.assertEquals(Collections.singletonList("auditor"), rolesOf(policy.id()));
  }

  @TestTemplate
  public void testLegacyCleanupRemovesReferencesOfADeletedPolicy()
      throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy = accessControlPolicy("reader", Arrays.asList("analyst", "auditor"));
    backend.insert(policy, false);

    // A policy delete still soft-deletes its references, so legacy cleanup has rows to collect.
    backend.delete(policy.nameIdentifier(), Entity.EntityType.POLICY, false);
    Assertions.assertEquals(2L, softDeletedReferenceCount());

    PolicyMetaService.getInstance()
        .deletePolicyAndVersionMetasByLegacyTimeline(System.currentTimeMillis() + 1000L, 10);

    Assertions.assertEquals(0L, softDeletedReferenceCount());
    Assertions.assertEquals(0L, activeReferenceCount(policy.id()));
  }

  /** Deletes the metalake without cascade on another thread and waits for that thread to end. */
  private void deleteMetalakeWithoutCascade(AtomicReference<Throwable> failure) {
    Thread deleter =
        new Thread(
            () -> {
              try {
                backend.delete(NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE, false);
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    deleter.start();
    try {
      deleter.join(30_000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for the metalake delete", e);
    }
    Assertions.assertFalse(
        deleter.isAlive(), "The metalake delete should not block on the paused update");
  }

  private RoleEntity role(CatalogEntity catalog, String name) {
    return createRoleEntity(
        RandomIdGenerator.INSTANCE.nextId(),
        AuthorizationUtils.ofRoleNamespace(METALAKE),
        name,
        AUDIT_INFO,
        catalog.name());
  }

  private PolicyEntity accessControlPolicy(String name, List<String> roles) {
    return PolicyEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
        .withName(name)
        .withPolicyType(Policy.BuiltInType.ACCESS_CONTROL)
        .withComment("")
        .withEnabled(true)
        .withContent(
            PolicyContents.accessControl(
                Collections.singletonList(Privilege.Name.SELECT_TABLE), roles))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static PolicyEntity withRoles(PolicyEntity policy, List<String> roles) {
    return PolicyEntity.builder()
        .withId(policy.id())
        .withNamespace(policy.namespace())
        .withName(policy.name())
        .withPolicyType(policy.policyType())
        .withComment(policy.comment())
        .withEnabled(policy.enabled())
        .withContent(
            PolicyContents.accessControl(
                Collections.singletonList(Privilege.Name.SELECT_TABLE), roles))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private List<String> rolesOf(long policyId) {
    return SessionUtils.getWithoutCommit(
            PolicyRoleRelMapper.class, mapper -> mapper.listByPolicyId(policyId))
        .stream()
        .map(PolicyRoleRelPO::getRoleName)
        .collect(Collectors.toList());
  }

  private List<String> policiesReferencing(String roleName) {
    return SessionUtils.getWithoutCommit(
            PolicyRoleRelMapper.class, mapper -> mapper.listByRoleName(METALAKE, roleName))
        .stream()
        .map(PolicyRoleRelPO::getPolicyName)
        .collect(Collectors.toList());
  }

  private PolicyRoleRelPO referenceTo(long policyId, String roleName) {
    return SessionUtils.getWithoutCommit(
            PolicyRoleRelMapper.class, mapper -> mapper.listByPolicyId(policyId))
        .stream()
        .filter(reference -> roleName.equals(reference.getRoleName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No reference to role " + roleName));
  }

  private long softDeletedReferenceCount() throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT COUNT(*) FROM policy_role_relation_meta WHERE deleted_at > 0")) {
      Assertions.assertTrue(resultSet.next());
      return resultSet.getLong(1);
    }
  }

  private long activeReferenceCount(long policyId) throws SQLException {
    return count(
        "SELECT COUNT(*) FROM policy_role_relation_meta WHERE policy_id = ? AND deleted_at = 0",
        policyId);
  }

  private long count(String sql, long policyId) throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setLong(1, policyId);
      try (ResultSet resultSet = statement.executeQuery()) {
        Assertions.assertTrue(resultSet.next());
        return resultSet.getLong(1);
      }
    }
  }
}
