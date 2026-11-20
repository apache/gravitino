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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.bulk.BulkItemResult;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.PolicyRoleRelMapper;
import org.apache.gravitino.storage.relational.po.PolicyRoleRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

/**
 * Tests that a role cannot be deleted while an access control policy names it, and that a policy
 * cannot name a role that does not exist.
 *
 * <p>The two-transaction tests hold one write open until the other is waiting on it, then commit,
 * so each covers one commit order between a policy write and a role deletion.
 */
public class TestReferencedRoleDeletion extends TestJDBCBackend {

  private static final String METALAKE = "referenced_role_metalake";

  private static LockManager lockManager;

  private Object previousLockManager;
  private Object previousCatalogManager;
  private AccessControlManager accessControlManager;

  @BeforeEach
  void setUpAccessControl() throws IllegalAccessException {
    Config config = new Config(false) {};
    config.set(Configs.SERVICE_ADMINS, Collections.singletonList("admin"));
    // Bulk deletion takes the in-memory tree lock before it deletes each role.
    if (lockManager == null) {
      lockManager = new LockManager(config);
    }
    previousLockManager = FieldUtils.readField(GravitinoEnv.getInstance(), "lockManager", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", lockManager, true);
    // A deleted role is passed to the catalogs' authorization plugins. These roles hold no
    // privileges, so no catalog is consulted, but the lookup needs a catalog manager to exist.
    previousCatalogManager =
        FieldUtils.readField(GravitinoEnv.getInstance(), "catalogManager", true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "catalogManager", Mockito.mock(CatalogManager.class), true);
    accessControlManager =
        new AccessControlManager(
            RelationalEntityStore.forOfflineMigration(backend, config),
            RandomIdGenerator.INSTANCE,
            config);
  }

  @AfterEach
  void restoreEnvironment() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", previousLockManager, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "catalogManager", previousCatalogManager, true);
  }

  @TestTemplate
  public void testPolicyCannotNameMissingRole() throws IOException, SQLException {
    createAndInsertMakeLake(METALAKE);
    PolicyEntity policy = policy("reader", "analyst");

    for (boolean overwritten : new boolean[] {false, true}) {
      IllegalRoleException failure =
          Assertions.assertThrows(
              IllegalRoleException.class, () -> backend.insert(policy, overwritten));
      Assertions.assertTrue(failure.getMessage().contains("analyst"), failure.getMessage());
    }

    Assertions.assertEquals(0L, countAllPolicyRows("policy_meta", policy.id()));
    Assertions.assertEquals(0L, countAllPolicyRows("policy_version_info", policy.id()));
    Assertions.assertEquals(0L, countAllPolicyRows("policy_role_relation_meta", policy.id()));
  }

  @TestTemplate
  public void testPolicyUpdateCannotNameMissingRole() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createRoles("analyst");
    PolicyEntity policy = policy("reader", "analyst");
    backend.insert(policy, false);
    int versionsBefore = countPolicyRows("policy_version_info", policy.id());

    Assertions.assertThrows(
        IllegalRoleException.class, () -> updateRoles(policy, "analyst", "auditor"));

    PolicyEntity stored = backend.get(policy.nameIdentifier(), Entity.EntityType.POLICY);
    Assertions.assertEquals(policy.content(), stored.content());
    Assertions.assertEquals(versionsBefore, countPolicyRows("policy_version_info", policy.id()));
    Assertions.assertEquals(Collections.singletonList("analyst"), rolesOf(policy.id()));
  }

  @TestTemplate
  public void testDeletingReferencedRoleIsRefused() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createRoles("analyst");
    backend.insert(policy("reader", "analyst"), false);

    InUseException failure =
        Assertions.assertThrows(InUseException.class, () -> deleteRole("analyst"));

    Assertions.assertTrue(failure.getMessage().contains("analyst"), failure.getMessage());
    // The caller may not be allowed to see the policy, so the error does not name it.
    Assertions.assertFalse(failure.getMessage().contains("reader"), failure.getMessage());
    Assertions.assertTrue(roleExists("analyst"));
  }

  @TestTemplate
  public void testBulkDeletionRefusesOnlyReferencedRoles() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createRoles("analyst", "auditor");
    backend.insert(policy("reader", "analyst"), false);

    List<BulkItemResult<String>> results =
        accessControlManager.deleteRoles(METALAKE, Arrays.asList("analyst", "auditor"));

    assertRefused(results.get(0));
    assertSucceeded(results.get(1));
    Assertions.assertTrue(roleExists("analyst"));
    Assertions.assertFalse(roleExists("auditor"));
  }

  @TestTemplate
  public void testRoleCanBeDeletedOnceNoPolicyNamesIt() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createRoles("analyst", "auditor");
    PolicyEntity reader = policy("reader", "analyst");
    PolicyEntity writer = policy("writer", "analyst");
    backend.insert(reader, false);
    backend.insert(writer, false);

    updateRoles(reader, "auditor");
    Assertions.assertThrows(InUseException.class, () -> deleteRole("analyst"));

    Assertions.assertTrue(backend.delete(writer.nameIdentifier(), Entity.EntityType.POLICY, false));
    Assertions.assertTrue(deleteRole("analyst"));
    Assertions.assertFalse(roleExists("analyst"));
  }

  @TestTemplate
  public void testReferenceInAnotherMetalakeDoesNotBlockDeletion() throws IOException {
    createAndInsertMakeLake(METALAKE);
    String otherMetalake = METALAKE + "_other";
    createAndInsertMakeLake(otherMetalake);
    createRoles("analyst");
    createRolesIn(otherMetalake, "analyst");
    backend.insert(policyIn(otherMetalake, "reader", "analyst"), false);

    Assertions.assertTrue(deleteRole("analyst"));
    Assertions.assertThrows(
        InUseException.class,
        () ->
            backend.delete(
                AuthorizationUtils.ofRole(otherMetalake, "analyst"),
                Entity.EntityType.ROLE,
                false));
  }

  @TestTemplate
  public void testMetalakeDeleteIsNotBlockedByReferences() throws IOException {
    for (boolean cascade : new boolean[] {true, false}) {
      String metalake = METALAKE + (cascade ? "_cascade" : "_plain");
      createAndInsertMakeLake(metalake);
      createRolesIn(metalake, "analyst");
      PolicyEntity policy = policyIn(metalake, "reader", "analyst");
      backend.insert(policy, false);

      Assertions.assertTrue(
          backend.delete(NameIdentifier.of(metalake), Entity.EntityType.METALAKE, cascade));

      Assertions.assertEquals(0, countPolicyRows("policy_meta", policy.id()));
      Assertions.assertEquals(0, countPolicyRows("policy_role_relation_meta", policy.id()));
      Assertions.assertEquals(
          0, countActiveRowsWhere("role_meta", String.format("role_name = '%s'", "analyst")));
    }
  }

  @TestTemplate
  public void testDeletionWaitingOnPolicyCreateIsRefused() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createRoles("analyst");
    PolicyEntity policy = policy("reader", "analyst");

    Throwable failure = whileHeld(() -> backend.insert(policy, false), () -> deleteRole("analyst"));

    Assertions.assertInstanceOf(InUseException.class, failure);
    Assertions.assertTrue(roleExists("analyst"));
    Assertions.assertEquals(Collections.singletonList("analyst"), rolesOf(policy.id()));
  }

  @TestTemplate
  public void testDeletionWaitingOnPolicyUpdateIsRefused() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createRoles("analyst", "auditor");
    PolicyEntity policy = policy("reader", "auditor");
    backend.insert(policy, false);

    Throwable failure =
        whileHeld(() -> updateRoles(policy, "auditor", "analyst"), () -> deleteRole("analyst"));

    Assertions.assertInstanceOf(InUseException.class, failure);
    Assertions.assertTrue(roleExists("analyst"));
    Assertions.assertEquals(Arrays.asList("analyst", "auditor"), rolesOf(policy.id()));
  }

  @TestTemplate
  public void testBulkDeletionWaitingOnPolicyWriteIsRefused() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createRoles("analyst", "auditor");
    PolicyEntity policy = policy("reader", "analyst");
    AtomicReference<List<BulkItemResult<String>>> results = new AtomicReference<>();

    Throwable failure =
        whileHeld(
            () -> backend.insert(policy, false),
            () ->
                results.set(
                    accessControlManager.deleteRoles(
                        METALAKE, Arrays.asList("analyst", "auditor"))));

    Assertions.assertNull(failure);
    assertRefused(results.get().get(0));
    assertSucceeded(results.get().get(1));
    Assertions.assertTrue(roleExists("analyst"));
    Assertions.assertFalse(roleExists("auditor"));
    Assertions.assertEquals(Collections.singletonList("analyst"), rolesOf(policy.id()));
  }

  @TestTemplate
  public void testPolicyCreateWaitingOnDeletionFails() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createRoles("analyst");
    PolicyEntity policy = policy("reader", "analyst");

    Throwable failure =
        whileHeld(
            () -> Assertions.assertTrue(deleteRole("analyst")),
            () -> backend.insert(policy, false));

    Assertions.assertInstanceOf(IllegalRoleException.class, failure);
    Assertions.assertFalse(roleExists("analyst"));
    Assertions.assertEquals(0L, countAllPolicyRows("policy_meta", policy.id()));
    Assertions.assertEquals(0L, countAllPolicyRows("policy_version_info", policy.id()));
    Assertions.assertEquals(0L, countAllPolicyRows("policy_role_relation_meta", policy.id()));
  }

  @TestTemplate
  public void testPolicyUpdateWaitingOnDeletionFails() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createRoles("analyst", "auditor");
    PolicyEntity policy = policy("reader", "auditor");
    backend.insert(policy, false);
    int versionsBefore = countPolicyRows("policy_version_info", policy.id());

    Throwable failure =
        whileHeld(
            () -> Assertions.assertTrue(deleteRole("analyst")),
            () -> updateRoles(policy, "auditor", "analyst"));

    Assertions.assertInstanceOf(IllegalRoleException.class, failure);
    Assertions.assertFalse(roleExists("analyst"));
    PolicyEntity stored = backend.get(policy.nameIdentifier(), Entity.EntityType.POLICY);
    Assertions.assertEquals(policy.content(), stored.content());
    Assertions.assertEquals(versionsBefore, countPolicyRows("policy_version_info", policy.id()));
    Assertions.assertEquals(Collections.singletonList("auditor"), rolesOf(policy.id()));
  }

  @TestTemplate
  public void testPolicyWriteWaitingOnBulkDeletionFails() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createRoles("analyst");
    PolicyEntity policy = policy("reader", "analyst");

    Throwable failure =
        whileHeld(
            () ->
                assertSucceeded(
                    accessControlManager
                        .deleteRoles(METALAKE, Collections.singletonList("analyst"))
                        .get(0)),
            () -> backend.insert(policy, false));

    Assertions.assertInstanceOf(IllegalRoleException.class, failure);
    Assertions.assertFalse(roleExists("analyst"));
    Assertions.assertEquals(0L, countAllPolicyRows("policy_meta", policy.id()));
    Assertions.assertEquals(0L, countAllPolicyRows("policy_role_relation_meta", policy.id()));
  }

  /**
   * Runs {@code first} in a transaction, starts {@code second} in another one, waits until the
   * database reports {@code second} waiting on {@code first}, and then commits {@code first}.
   *
   * @return the failure of {@code second}, or null if it committed
   */
  private Throwable whileHeld(Executable first, Executable second) throws Exception {
    return new TransactionLockWaits(backendType)
        .whileTransactionHeld(first, second, () -> {}, true);
  }

  private static void assertSucceeded(BulkItemResult<String> result) {
    Assertions.assertTrue(
        result.succeeded(), () -> "Unexpected failure: " + result.error().orElse(null));
  }

  private static void assertRefused(BulkItemResult<String> result) {
    Assertions.assertFalse(result.succeeded());
    Assertions.assertInstanceOf(InUseException.class, result.error().orElse(null));
  }

  private boolean deleteRole(String roleName) throws IOException {
    return backend.delete(
        AuthorizationUtils.ofRole(METALAKE, roleName), Entity.EntityType.ROLE, false);
  }

  private boolean roleExists(String roleName) throws IOException {
    return backend.exists(AuthorizationUtils.ofRole(METALAKE, roleName), Entity.EntityType.ROLE);
  }

  private void createRoles(String... roleNames) throws IOException {
    createRolesIn(METALAKE, roleNames);
  }

  private void createRolesIn(String metalake, String... roleNames) throws IOException {
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

  private void updateRoles(PolicyEntity policy, String... roleNames) throws IOException {
    backend.update(
        policy.nameIdentifier(),
        Entity.EntityType.POLICY,
        entity -> withRoles(policy, Arrays.asList(roleNames)));
  }

  private PolicyEntity policy(String name, String... roleNames) {
    return policyIn(METALAKE, name, roleNames);
  }

  private PolicyEntity policyIn(String metalake, String name, String... roleNames) {
    return PolicyEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withNamespace(NamespaceUtil.ofPolicy(metalake))
        .withName(name)
        .withPolicyType(Policy.BuiltInType.ACCESS_CONTROL)
        .withComment("")
        .withEnabled(true)
        .withContent(accessControl(Arrays.asList(roleNames)))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static PolicyEntity withRoles(PolicyEntity policy, List<String> roleNames) {
    return PolicyEntity.builder()
        .withId(policy.id())
        .withNamespace(policy.namespace())
        .withName(policy.name())
        .withPolicyType(policy.policyType())
        .withComment(policy.comment())
        .withEnabled(policy.enabled())
        .withContent(accessControl(roleNames))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static PolicyContent accessControl(List<String> roleNames) {
    return PolicyContents.accessControl(
        Collections.singletonList(Privilege.Name.SELECT_TABLE), roleNames);
  }

  private List<String> rolesOf(long policyId) {
    return SessionUtils.getWithoutCommit(
            PolicyRoleRelMapper.class, mapper -> mapper.listByPolicyId(policyId))
        .stream()
        .map(PolicyRoleRelPO::getRoleName)
        .collect(Collectors.toList());
  }

  private int countPolicyRows(String table, long policyId) {
    return countActiveRowsWhere(table, "policy_id = " + policyId);
  }

  /** Counts rows whether live or soft-deleted, for writes that must have committed nothing. */
  private long countAllPolicyRows(String table, long policyId) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        PreparedStatement statement =
            connection.prepareStatement("SELECT COUNT(*) FROM " + table + " WHERE policy_id = ?")) {
      statement.setLong(1, policyId);
      try (ResultSet rows = statement.executeQuery()) {
        Assertions.assertTrue(rows.next());
        return rows.getLong(1);
      }
    }
  }
}
