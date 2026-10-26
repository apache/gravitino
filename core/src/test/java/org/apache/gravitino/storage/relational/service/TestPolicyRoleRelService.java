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
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.meta.PolicyEntity;
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
      Thread.sleep(400);
      Assertions.assertTrue(deleter.isAlive(), "The metalake delete should wait on the policy row");

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
  public void testLegacyCleanupRemovesSupersededReferences() throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy = accessControlPolicy("reader", Arrays.asList("analyst", "auditor"));
    backend.insert(policy, false);

    backend.update(
        policy.nameIdentifier(),
        Entity.EntityType.POLICY,
        entity -> withRoles(policy, Collections.singletonList("auditor")));
    Assertions.assertEquals(1L, softDeletedReferenceCount());

    PolicyMetaService.getInstance()
        .deletePolicyAndVersionMetasByLegacyTimeline(System.currentTimeMillis() + 1000L, 10);

    Assertions.assertEquals(0L, softDeletedReferenceCount());
    Assertions.assertEquals(Collections.singletonList("auditor"), rolesOf(policy.id()));
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
