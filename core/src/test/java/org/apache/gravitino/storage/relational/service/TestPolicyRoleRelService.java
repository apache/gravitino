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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
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
    createRoles(METALAKE, "analyst", "auditor", "steward");
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
    createRoles(METALAKE, "analyst", "auditor");
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
    createRoles(METALAKE, "analyst", "steward");
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
    String oversizedRoleName = "r".repeat(EntityFieldLimits.MAX_NAME_LENGTH + 1);
    PolicyEntity policy =
        accessControlPolicy("reader", Collections.singletonList(oversizedRoleName));

    IllegalArgumentException failure =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> backend.insert(policy, false));
    Assertions.assertTrue(
        failure.getMessage().contains("must not exceed " + EntityFieldLimits.MAX_NAME_LENGTH),
        failure.getMessage());

    Assertions.assertEquals(
        0L, count("SELECT COUNT(*) FROM policy_meta WHERE policy_id = ?", policy.id()));
    Assertions.assertEquals(
        0L, count("SELECT COUNT(*) FROM policy_version_info WHERE policy_id = ?", policy.id()));
    Assertions.assertEquals(
        0L,
        count("SELECT COUNT(*) FROM policy_role_relation_meta WHERE policy_id = ?", policy.id()));
  }

  @TestTemplate
  public void testRoleNameAtTheLengthLimitIsAccepted() throws IOException {
    createAndInsertMakeLake(METALAKE);
    String longestRoleName = "r".repeat(EntityFieldLimits.MAX_NAME_LENGTH);
    createRoles(METALAKE, longestRoleName);
    PolicyEntity policy = accessControlPolicy("reader", Collections.singletonList(longestRoleName));
    backend.insert(policy, false);

    Assertions.assertEquals(Collections.singletonList(longestRoleName), rolesOf(policy.id()));
  }

  @TestTemplate
  public void testFailedReferenceUpdateKeepsThePreviousPolicy() throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    createRoles(METALAKE, "analyst", "auditor");
    PolicyEntity policy = accessControlPolicy("reader", Collections.singletonList("analyst"));
    backend.insert(policy, false);
    String versionsOfPolicy = "SELECT COUNT(*) FROM policy_version_info WHERE policy_id = ?";
    long versionsBefore = count(versionsOfPolicy, policy.id());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            backend.update(
                policy.nameIdentifier(),
                Entity.EntityType.POLICY,
                entity ->
                    withRoles(
                        policy,
                        Arrays.asList(
                            "auditor", "r".repeat(EntityFieldLimits.MAX_NAME_LENGTH + 1)))));

    PolicyEntity stored = backend.get(policy.nameIdentifier(), Entity.EntityType.POLICY);
    Assertions.assertEquals(policy.content(), stored.content());
    Assertions.assertEquals(versionsBefore, count(versionsOfPolicy, policy.id()));
    Assertions.assertEquals(Collections.singletonList("analyst"), rolesOf(policy.id()));
  }

  @TestTemplate
  public void testDuplicateRoleNamesCollapseToOneReference() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createRoles(METALAKE, "analyst");
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
    createRoles(METALAKE, "analyst");
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
    createRoles(METALAKE, "analyst");
    createRoles(otherMetalake, "analyst");

    PolicyEntity policy = accessControlPolicy("reader", Collections.singletonList("analyst"));
    backend.insert(policy, false);
    PolicyEntity otherPolicy =
        accessControlPolicy(otherMetalake, "reader", Collections.singletonList("analyst"));
    backend.insert(otherPolicy, false);

    Assertions.assertTrue(
        backend.delete(NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE, true));

    Assertions.assertEquals(0L, activeReferenceCount(policy.id()));
    Assertions.assertEquals(1L, activeReferenceCount(otherPolicy.id()));
  }

  @TestTemplate
  public void testRoleLookupStaysInItsMetalake() throws IOException {
    createAndInsertMakeLake(METALAKE);
    String otherMetalake = METALAKE + "_other";
    createAndInsertMakeLake(otherMetalake);
    createRoles(METALAKE, "analyst");
    createRoles(otherMetalake, "analyst");
    backend.insert(accessControlPolicy("reader", Collections.singletonList("analyst")), false);
    backend.insert(
        accessControlPolicy(otherMetalake, "writer", Collections.singletonList("analyst")), false);

    Assertions.assertEquals(
        Collections.singletonList("reader"), policiesReferencing(METALAKE, "analyst"));
    Assertions.assertEquals(
        Collections.singletonList("writer"), policiesReferencing(otherMetalake, "analyst"));

    Assertions.assertTrue(
        backend.delete(NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE, true));

    Assertions.assertEquals(Collections.emptyList(), policiesReferencing(METALAKE, "analyst"));
    Assertions.assertEquals(
        Collections.singletonList("writer"), policiesReferencing(otherMetalake, "analyst"));
  }

  @TestTemplate
  public void testMetalakeDeleteClearsReferencesOfAnInFlightPolicyUpdate() throws Exception {
    assertMetalakeDeleteClearsReferenceAddedWhileWaiting(true);
  }

  @TestTemplate
  public void testNonCascadeMetalakeDeleteClearsReferencesOfAnInFlightPolicyUpdate()
      throws Exception {
    assertMetalakeDeleteClearsReferenceAddedWhileWaiting(false);
  }

  /**
   * Covers an update that read the policy before the metalake was deleted and writes after the
   * delete committed. It must fail as not found, leave the policy deleted, and write no reference.
   */
  @TestTemplate
  public void testUpdateReadBeforeMetalakeDeleteFailsWithoutReferences() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createRoles(METALAKE, "analyst", "auditor");
    PolicyEntity policy = accessControlPolicy("reader", Collections.singletonList("analyst"));
    backend.insert(policy, false);

    // The updater callback runs after the update read the policy and before its write transaction
    // opens, so the delete commits in between.
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            backend.update(
                policy.nameIdentifier(),
                Entity.EntityType.POLICY,
                entity -> {
                  deleteMetalakeOnAnotherThread();
                  return withRoles(policy, Arrays.asList("analyst", "auditor"));
                }));

    Assertions.assertEquals(
        0L,
        count(
            "SELECT COUNT(*) FROM policy_meta WHERE policy_id = ? AND deleted_at = 0",
            policy.id()));
    Assertions.assertEquals(0L, activeReferenceCount(policy.id()));
  }

  @TestTemplate
  public void testSupersededReferencesLeaveNoTombstone() throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    createRoles(METALAKE, "analyst", "auditor");
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
  public void testLegacyCleanupRemovesOnlyReferencesOfDeletedPolicies()
      throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    createRoles(METALAKE, "analyst", "auditor");
    PolicyEntity deleted = accessControlPolicy("reader", Arrays.asList("analyst", "auditor"));
    PolicyEntity live = accessControlPolicy("writer", Collections.singletonList("analyst"));
    backend.insert(deleted, false);
    backend.insert(live, false);

    // A policy delete soft-deletes its references, so legacy cleanup has rows to collect.
    backend.delete(deleted.nameIdentifier(), Entity.EntityType.POLICY, false);
    Assertions.assertEquals(2L, softDeletedReferenceCount());

    // A cutoff past every timestamp avoids comparing the JVM clock with the database clock. A limit
    // of one stops the first run at its LIMIT, which PostgreSQL writes differently.
    PolicyMetaService service = PolicyMetaService.getInstance();
    service.deletePolicyAndVersionMetasByLegacyTimeline(Long.MAX_VALUE, 1);
    Assertions.assertEquals(1L, softDeletedReferenceCount());

    // The next run has room for every row, and must still leave the live policy's reference alone.
    service.deletePolicyAndVersionMetasByLegacyTimeline(Long.MAX_VALUE, 10);
    Assertions.assertEquals(0L, softDeletedReferenceCount());
    Assertions.assertEquals(1L, activeReferenceCount(live.id()));
    Assertions.assertEquals(Collections.singletonList("analyst"), rolesOf(live.id()));
  }

  /**
   * Holds the metalake row the way an in-flight policy update does, waits until the metalake delete
   * is blocked on that row, and only then lets the update add a reference and commit.
   *
   * <p>A policy update locks the metalake before the roles it names and its own row, the order the
   * metalake delete locks them in. The delete therefore cannot run any of its cleanup until the
   * update commits, and then clears the reference the update added.
   */
  private void assertMetalakeDeleteClearsReferenceAddedWhileWaiting(boolean cascade)
      throws Exception {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE);
    createRoles(METALAKE, "analyst", "auditor");
    PolicyEntity policy = accessControlPolicy("reader", Collections.singletonList("analyst"));
    backend.insert(policy, false);

    Throwable deleteFailure =
        new TransactionLockWaits(backendType)
            .whileTransactionHeld(
                () ->
                    LiveEndpointService.lockLiveEndpoint(
                        NameIdentifier.of(METALAKE),
                        Entity.EntityType.METALAKE,
                        new NamespacedEntityId(metalake.id())),
                () ->
                    backend.delete(
                        NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE, cascade),
                () ->
                    backend.update(
                        policy.nameIdentifier(),
                        Entity.EntityType.POLICY,
                        entity -> withRoles(policy, Arrays.asList("analyst", "auditor"))),
                true);

    Assertions.assertNull(deleteFailure);
    Assertions.assertEquals(0L, activeReferenceCount(policy.id()));
  }

  /** Deletes the metalake without cascade on another thread and waits for it to finish. */
  private void deleteMetalakeOnAnotherThread() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Assertions.assertTrue(
          executor
              .submit(
                  () ->
                      backend.delete(
                          NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE, false))
              .get(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for the metalake delete", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException("The metalake delete did not finish", e);
    } finally {
      executor.shutdownNow();
    }
  }

  /** Creates live roles, without privileges, for policies to name. */
  private void createRoles(String metalake, String... roleNames) throws IOException {
    for (String roleName : roleNames) {
      backend.insert(
          createRoleEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              AuthorizationUtils.ofRoleNamespace(metalake),
              roleName,
              AUDIT_INFO,
              Collections.emptyList(),
              null),
          false);
    }
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
    return accessControlPolicy(METALAKE, name, roles);
  }

  private PolicyEntity accessControlPolicy(String metalake, String name, List<String> roles) {
    return PolicyEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withNamespace(NamespaceUtil.ofPolicy(metalake))
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
    return policiesReferencing(METALAKE, roleName);
  }

  private List<String> policiesReferencing(String metalake, String roleName) {
    return SessionUtils.getWithoutCommit(
            PolicyRoleRelMapper.class, mapper -> mapper.listByRoleName(metalake, roleName))
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
