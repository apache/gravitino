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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyVersionMapper;
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestPolicyMetaService extends TestJDBCBackend {
  private static final String METALAKE_NAME = "metalake_for_policy_test";

  private final Set<MetadataObject.Type> supportedObjectTypes =
      ImmutableSet.of(
          MetadataObject.Type.CATALOG,
          MetadataObject.Type.SCHEMA,
          MetadataObject.Type.TABLE,
          MetadataObject.Type.FILESET,
          MetadataObject.Type.MODEL,
          MetadataObject.Type.TOPIC);
  private final PolicyContent content =
      PolicyContents.custom(ImmutableMap.of("filed1", 123), supportedObjectTypes, null);

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy",
            AUDIT_INFO);
    PolicyEntity policyCopy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy",
            AUDIT_INFO);
    backend.insert(policy, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(policyCopy, false));
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(metalake.name()),
            "policy",
            AUDIT_INFO);
    PolicyEntity policy1 =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(metalake.name()),
            "policy1",
            AUDIT_INFO);
    backend.insert(policy, false);
    backend.insert(policy1, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                policy1.nameIdentifier(),
                Entity.EntityType.POLICY,
                e -> createPolicy(policy1.id(), policy1.namespace(), "policy", AUDIT_INFO)));
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);

    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(metalake.name()),
            "policy",
            AUDIT_INFO);
    backend.insert(policy, false);
    // update policy enabled and version
    PolicyEntity policyV2 =
        PolicyEntity.builder()
            .withId(policy.id())
            .withNamespace(policy.namespace())
            .withName(policy.name())
            .withPolicyType(policy.policyType())
            .withComment(policy.comment())
            .withEnabled(!policy.enabled())
            .withContent(policy.content())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.update(policy.nameIdentifier(), Entity.EntityType.POLICY, e -> policyV2);

    // another meta data creation
    String anotherMetaLakeName = METALAKE_NAME + "_another";
    BaseMetalake anotherMetaLake = createAndInsertMakeLake(anotherMetaLakeName);

    PolicyEntity anotherPolicy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(anotherMetaLake.name()),
            "another-policy",
            AUDIT_INFO);
    backend.insert(anotherPolicy, false);

    // update another policy enabled and version
    PolicyEntity anotherPolicyV2 =
        PolicyEntity.builder()
            .withId(anotherPolicy.id())
            .withNamespace(anotherPolicy.namespace())
            .withName(anotherPolicy.name())
            .withPolicyType(anotherPolicy.policyType())
            .withComment(anotherPolicy.comment())
            .withEnabled(!anotherPolicy.enabled())
            .withContent(anotherPolicy.content())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.update(anotherPolicy.nameIdentifier(), Entity.EntityType.POLICY, e -> anotherPolicyV2);
    // update another policy comment and version
    PolicyEntity anotherPolicyV3 =
        PolicyEntity.builder()
            .withId(anotherPolicy.id())
            .withNamespace(anotherPolicy.namespace())
            .withName(anotherPolicy.name())
            .withPolicyType(anotherPolicy.policyType())
            .withComment("v3")
            .withEnabled(anotherPolicyV2.enabled())
            .withContent(anotherPolicy.content())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.update(anotherPolicy.nameIdentifier(), Entity.EntityType.POLICY, e -> anotherPolicyV3);

    List<PolicyEntity> policies = backend.list(policy.namespace(), Entity.EntityType.POLICY, true);
    assertFalse(policies.contains(policy));
    assertTrue(policies.contains(policyV2));
    assertEquals(policyV2.enabled(), policies.get(policies.indexOf(policyV2)).enabled());

    // meta data soft delete
    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true);

    // check existence after soft delete
    assertFalse(backend.exists(policy.nameIdentifier(), Entity.EntityType.POLICY));
    assertTrue(backend.exists(anotherPolicy.nameIdentifier(), Entity.EntityType.POLICY));
    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(policy.id(), Entity.EntityType.POLICY));
    assertEquals(2, listPolicyVersions(policy.id()).size());
    assertEquals(3, listPolicyVersions(anotherPolicy.id()).size());

    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(policy.id(), Entity.EntityType.POLICY));
    assertEquals(0, listPolicyVersions(policy.id()).size());
    Map<Integer, Long> anotherPolicyVersionsAfterHardDelete =
        listPolicyVersions(anotherPolicy.id());
    assertTrue(anotherPolicyVersionsAfterHardDelete.containsKey(3));
    assertEquals(0L, anotherPolicyVersionsAfterHardDelete.get(3));

    // soft delete for old version policy
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.deleteOldVersionData(entityType, 1);
    }
    Map<Integer, Long> versionDeletedMap2 = listPolicyVersions(anotherPolicy.id());
    assertTrue(versionDeletedMap2.containsKey(3));
    assertEquals(0L, versionDeletedMap2.get(3));
    assertEquals(1, versionDeletedMap2.values().stream().filter(value -> value == 0L).count());
  }

  @TestTemplate
  public void testInsertAndGetPolicyByIdentifier() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);

    // Test no policy entity.
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                policyMetaService.getPolicyByIdentifier(
                    NameIdentifierUtil.ofPolicy(metalake.name(), "policy1")));
    assertEquals("No such policy entity: policy1", excep.getMessage());

    // Test get policy entity
    PolicyEntity policyEntity =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(metalake.name()))
            .withComment("comment")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withEnabled(true)
            .withAuditInfo(AUDIT_INFO)
            .build();
    policyMetaService.insertPolicy(policyEntity, false);

    PolicyEntity resultpolicyEntity =
        policyMetaService.getPolicyByIdentifier(
            NameIdentifierUtil.ofPolicy(metalake.name(), "policy1"));
    assertEquals(policyEntity, resultpolicyEntity);

    // Test with null comment and content properties.
    PolicyEntity PolicyEntity1 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy2")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withNamespace(NamespaceUtil.ofPolicy(metalake.name()))
            .withAuditInfo(AUDIT_INFO)
            .withContent(content)
            .build();

    policyMetaService.insertPolicy(PolicyEntity1, false);
    PolicyEntity resultPolicyEntity1 =
        policyMetaService.getPolicyByIdentifier(
            NameIdentifierUtil.ofPolicy(metalake.name(), "policy2"));
    assertEquals(PolicyEntity1, resultPolicyEntity1);
    Assertions.assertTrue(resultPolicyEntity1.enabled());
    Assertions.assertNull(resultPolicyEntity1.comment());
    Assertions.assertNull(resultPolicyEntity1.content().properties());

    // Test insert with overwrite.
    PolicyEntity PolicyEntity2 =
        PolicyEntity.builder()
            .withId(PolicyEntity1.id())
            .withName("policy3")
            .withNamespace(NamespaceUtil.ofPolicy(metalake.name()))
            .withComment("comment")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withAuditInfo(AUDIT_INFO)
            .build();

    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> policyMetaService.insertPolicy(PolicyEntity2, false));

    policyMetaService.insertPolicy(PolicyEntity2, true);

    PolicyEntity resultPolicyEntity2 =
        policyMetaService.getPolicyByIdentifier(
            NameIdentifierUtil.ofPolicy(metalake.name(), "policy3"));
    assertEquals(PolicyEntity2, resultPolicyEntity2);
  }

  @TestTemplate
  public void testCreateAndListPolicies() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policyEntity1 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE_NAME))
            .withComment("comment")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withAuditInfo(AUDIT_INFO)
            .build();
    policyMetaService.insertPolicy(policyEntity1, false);

    PolicyEntity policyEntity2 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy2")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE_NAME))
            .withComment("comment")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withAuditInfo(AUDIT_INFO)
            .build();
    policyMetaService.insertPolicy(policyEntity2, false);

    List<PolicyEntity> policyEntities =
        policyMetaService.listPoliciesByNamespace(NamespaceUtil.ofPolicy(METALAKE_NAME));
    assertEquals(2, policyEntities.size());
    Assertions.assertTrue(policyEntities.contains(policyEntity1));
    Assertions.assertTrue(policyEntities.contains(policyEntity2));
  }

  @TestTemplate
  public void testUpdatePolicy() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policyEntity1 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE_NAME))
            .withComment("comment")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withAuditInfo(AUDIT_INFO)
            .build();
    policyMetaService.insertPolicy(policyEntity1, false);

    // Update with no policy entity.
    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                policyMetaService.updatePolicy(
                    NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy2"),
                    policyEntity -> policyEntity));
    assertEquals("No such policy entity: policy2", excep.getMessage());

    // Update policy entity.
    PolicyEntity policyEntity2 =
        PolicyEntity.builder()
            .withId(policyEntity1.id())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE_NAME))
            .withComment("comment1")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withAuditInfo(AUDIT_INFO)
            .build();
    PolicyEntity updatedPolicyEntity =
        policyMetaService.updatePolicy(
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1"), policyEntity -> policyEntity2);
    assertEquals(policyEntity2, updatedPolicyEntity);

    PolicyEntity loadedPolicyEntity =
        policyMetaService.getPolicyByIdentifier(
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1"));
    assertEquals(policyEntity2, loadedPolicyEntity);

    // Update with different id.
    PolicyEntity policyEntity3 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE_NAME))
            .withComment("comment1")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withAuditInfo(AUDIT_INFO)
            .build();

    Exception excep1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                policyMetaService.updatePolicy(
                    NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1"),
                    policyEntity -> policyEntity3));
    assertEquals(
        "The updated policy entity id: "
            + policyEntity3.id()
            + " must have the same id as the old "
            + "entity id "
            + policyEntity2.id(),
        excep1.getMessage());

    PolicyEntity loadedPolicyEntity1 =
        policyMetaService.getPolicyByIdentifier(
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1"));
    assertEquals(policyEntity2, loadedPolicyEntity1);
  }

  @TestTemplate
  public void testMetadataOnlyPolicyAlterAdvancesOnlyTheOccVersion() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy_metadata_occ",
            AUDIT_INFO);
    policyMetaService.insertPolicy(policy, false);
    PolicyPO initialPO = getPolicyPO(policy.nameIdentifier());

    AuditInfo updatedAudit =
        AuditInfo.builder().withCreator("updated-creator").withCreateTime(Instant.now()).build();
    PolicyEntity metadataOnlyUpdate =
        copyPolicy(policy, policy.name(), policy.comment(), updatedAudit);
    policyMetaService.updatePolicy(policy.nameIdentifier(), ignored -> metadataOnlyUpdate);

    PolicyPO updatedPO = getPolicyPO(policy.nameIdentifier());
    // The audit info is the only thing that changed, and policy_version_info does not store it, so
    // the alter advances the OCC token alone and writes no snapshot.
    assertEquals(initialPO.getOccVersion() + 1, updatedPO.getOccVersion().longValue());
    assertEquals(initialPO.getCurrentVersion(), updatedPO.getCurrentVersion());
    assertEquals(initialPO.getLastVersion(), updatedPO.getLastVersion());
    assertNotEquals(initialPO.getAuditInfo(), updatedPO.getAuditInfo());

    // The row still points at the snapshot it already had, and reads still resolve it.
    assertEquals(updatedPO.getCurrentVersion(), updatedPO.getPolicyVersionPO().getVersion());
    assertEquals(policy.comment(), updatedPO.getPolicyVersionPO().getPolicyComment());
    assertEquals(policy.enabled(), updatedPO.getPolicyVersionPO().isEnabled());
    assertEquals(
        initialPO.getPolicyVersionPO().getContent(), updatedPO.getPolicyVersionPO().getContent());
    assertEquals(1, listPolicyVersions(policy.id()).size());
  }

  @TestTemplate
  public void testPolicyOverwriteAdvancesVersionAndRetainsHistory() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy_overwrite_occ",
            AUDIT_INFO);
    policyMetaService.insertPolicy(policy, false);
    PolicyPO initialPO = getPolicyPO(policy.nameIdentifier());

    PolicyEntity replacement = copyPolicy(policy, "policy_overwrite_occ_renamed", "replacement");
    policyMetaService.insertPolicy(replacement, true);

    PolicyPO overwrittenPO = getPolicyPO(replacement.nameIdentifier());
    assertEquals(initialPO.getCurrentVersion() + 1, overwrittenPO.getCurrentVersion().longValue());
    assertEquals(overwrittenPO.getCurrentVersion(), overwrittenPO.getLastVersion());
    assertEquals(2, listPolicyVersions(policy.id()).size());
    assertEquals(
        replacement, policyMetaService.getPolicyByIdentifier(replacement.nameIdentifier()));
  }

  @TestTemplate
  public void testPolicyAlterReportsOptimisticLockConflictWithoutOrphanVersion()
      throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy_alter_occ",
            AUDIT_INFO);
    policyMetaService.insertPolicy(policy, false);

    assertThrows(
        OptimisticLockException.class,
        () ->
            policyMetaService.updatePolicy(
                policy.nameIdentifier(),
                entity -> {
                  PolicyEntity current = (PolicyEntity) entity;
                  PolicyPO currentPO = getPolicyPO(current.nameIdentifier());
                  PolicyEntity competing = copyPolicy(current, current.name(), "competing");
                  PolicyPO competingPO =
                      POConverters.updatePolicyPOWithVersion(currentPO, competing);
                  SessionUtils.doMultipleWithCommit(
                      () ->
                          assertEquals(
                              Integer.valueOf(1),
                              SessionUtils.getWithoutCommit(
                                  PolicyMetaMapper.class,
                                  mapper -> mapper.updatePolicyMeta(competingPO, currentPO))),
                      () ->
                          SessionUtils.doWithoutCommit(
                              PolicyVersionMapper.class,
                              mapper ->
                                  mapper.insertPolicyVersion(competingPO.getPolicyVersionPO())));
                  return copyPolicy(current, current.name(), "requested");
                }));

    assertEquals(2, listPolicyVersions(policy.id()).size());
    assertEquals(
        "competing", policyMetaService.getPolicyByIdentifier(policy.nameIdentifier()).comment());
  }

  @TestTemplate
  public void testStalePolicyDeleteRollsBackRelationshipCleanup() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy_delete_occ",
            AUDIT_INFO);
    policyMetaService.insertPolicy(policy, false);
    PolicyPO stalePO = getPolicyPO(policy.nameIdentifier());
    policyMetaService.updatePolicy(
        policy.nameIdentifier(),
        entity -> copyPolicy((PolicyEntity) entity, ((PolicyEntity) entity).name(), "updated"));

    assertThrows(
        OptimisticLockException.class,
        () -> policyMetaService.deletePolicy(policy.nameIdentifier(), stalePO));
    assertTrue(backend.exists(policy.nameIdentifier(), Entity.EntityType.POLICY));
    assertEquals(
        2,
        listPolicyVersions(policy.id()).values().stream().filter(v -> v.longValue() == 0L).count());

    assertTrue(policyMetaService.deletePolicy(policy.nameIdentifier()));
    assertEquals(
        0,
        listPolicyVersions(policy.id()).values().stream().filter(v -> v.longValue() == 0L).count());
  }

  @TestTemplate
  public void testPolicyCreateIsFencedByParentMetalake() {
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy("metalake_that_does_not_exist"),
            "policy_without_metalake",
            AUDIT_INFO);

    assertThrows(NoSuchEntityException.class, () -> policyMetaService.insertPolicy(policy, false));
    assertThrows(NoSuchEntityException.class, () -> policyMetaService.insertPolicy(policy, true));
  }

  @TestTemplate
  public void testPolicyOverwriteReplacesTheRowHoldingTheName() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy_overwrite_by_name",
            AUDIT_INFO);
    policyMetaService.insertPolicy(policy, false);
    PolicyPO initialPO = getPolicyPO(policy.nameIdentifier());

    // A different ID for a name that is already taken replaces the row that holds the name,
    // instead of inserting a second row for it.
    PolicyEntity sameNameOtherId =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy_overwrite_by_name",
            AUDIT_INFO);
    policyMetaService.insertPolicy(sameNameOtherId, true);

    PolicyPO overwrittenPO = getPolicyPO(policy.nameIdentifier());
    assertEquals(policy.id(), overwrittenPO.getPolicyId().longValue());
    assertEquals(initialPO.getCurrentVersion() + 1, overwrittenPO.getCurrentVersion().longValue());
    assertEquals(2, listPolicyVersions(policy.id()).size());
  }

  /** A deleted primary key must not be classified as a retryable overwrite race. */
  @TestTemplate
  public void testOverwriteRejectsDeletedPolicyId() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyMetaService service = PolicyMetaService.getInstance();
    Namespace ns = NamespaceUtil.ofPolicy(METALAKE_NAME);
    PolicyEntity policy =
        createPolicy(RandomIdGenerator.INSTANCE.nextId(), ns, "deleted_policy_id", AUDIT_INFO);
    service.insertPolicy(policy, false);
    assertTrue(service.deletePolicy(policy.nameIdentifier()));
    EntityAlreadyExistsException failure =
        assertThrows(EntityAlreadyExistsException.class, () -> service.insertPolicy(policy, true));
    assertTrue(failure.getMessage().contains("use a new ID"));
    assertThrows(
        NoSuchEntityException.class, () -> service.getPolicyByIdentifier(policy.nameIdentifier()));
    listPolicyVersions(policy.id()).values().forEach(deletedAt -> assertTrue(deletedAt > 0));

    PolicyEntity replacement =
        createPolicy(RandomIdGenerator.INSTANCE.nextId(), ns, policy.name(), AUDIT_INFO);
    service.insertPolicy(replacement, true);
    assertEquals(replacement.id(), service.getPolicyByIdentifier(policy.nameIdentifier()).id());
  }

  /** An overwrite cannot adopt a stable ID from a metalake it has not locked. */
  @TestTemplate
  public void testOverwriteRejectsPolicyIdInAnotherMetalake() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    createAndInsertMakeLake("foreign_policy_metalake");
    PolicyMetaService service = PolicyMetaService.getInstance();
    PolicyEntity foreign =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy("foreign_policy_metalake"),
            "foreign_policy",
            AUDIT_INFO);
    service.insertPolicy(foreign, false);
    PolicyEntity incoming =
        createPolicy(
            foreign.id(), NamespaceUtil.ofPolicy(METALAKE_NAME), foreign.name(), AUDIT_INFO);
    assertThrows(EntityAlreadyExistsException.class, () -> service.insertPolicy(incoming, true));
    assertEquals(foreign.id(), service.getPolicyByIdentifier(foreign.nameIdentifier()).id());
    assertThrows(
        NoSuchEntityException.class,
        () -> service.getPolicyByIdentifier(incoming.nameIdentifier()));
  }

  /** Two first-time overwrites must serialize or expose a retryable insert conflict. */
  @TestTemplate
  public void testConcurrentOverwriteOfMissingPolicy() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyMetaService service = PolicyMetaService.getInstance();
    Namespace ns = NamespaceUtil.ofPolicy(METALAKE_NAME);
    PolicyEntity first =
        createPolicy(RandomIdGenerator.INSTANCE.nextId(), ns, "first_overwrite", AUDIT_INFO);
    PolicyEntity second =
        copyPolicy(
            createPolicy(RandomIdGenerator.INSTANCE.nextId(), ns, first.name(), AUDIT_INFO),
            first.name(),
            "second overwrite");
    CountDownLatch firstWritten = new CountDownLatch(1);
    CountDownLatch allowCommit = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> firstResult =
        executor.submit(
            () -> {
              SessionUtils.beginTransaction();
              try {
                service.insertPolicy(first, true);
                firstWritten.countDown();
                await(allowCommit);
                SessionUtils.commitTransaction();
                return null;
              } catch (Throwable failure) {
                SessionUtils.rollbackTransaction();
                return failure;
              }
            });
    try {
      assertTrue(firstWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> secondResult =
          executor.submit(
              () -> {
                secondStarted.countDown();
                try {
                  service.insertPolicy(second, true);
                  return null;
                } catch (Throwable failure) {
                  return failure;
                }
              });
      assertTrue(secondStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> secondResult.get(500, TimeUnit.MILLISECONDS));
      allowCommit.countDown();
      Assertions.assertNull(firstResult.get(30, TimeUnit.SECONDS));
      Throwable failure = secondResult.get(30, TimeUnit.SECONDS);
      if (failure != null) {
        Assertions.assertInstanceOf(OptimisticLockException.class, failure);
        assertEquals(1, listPolicyVersions(first.id()).size());
        assertTrue(listPolicyVersions(second.id()).isEmpty());
        service.insertPolicy(second, true);
      }
      PolicyEntity stored = service.getPolicyByIdentifier(first.nameIdentifier());
      assertEquals(first.id(), stored.id());
      assertEquals("second overwrite", stored.comment());
      assertEquals(2L, getPolicyPO(first.nameIdentifier()).getCurrentVersion());
      assertEquals(2, listPolicyVersions(first.id()).size());
      assertTrue(listPolicyVersions(second.id()).isEmpty());
    } finally {
      allowCommit.countDown();
      executor.shutdownNow();
    }
  }

  @TestTemplate
  public void testPolicyOverwriteByNameDoesNotRevertConcurrentRename() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity original =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy_overwrite_rename_race",
            AUDIT_INFO);
    policyMetaService.insertPolicy(original, false);
    PolicyPO observedPO = getPolicyPO(original.nameIdentifier());

    PolicyEntity renamed = copyPolicy(original, "policy_overwrite_rename_winner", "rename winner");
    PolicyPO renamedPO = POConverters.updatePolicyPOWithVersion(observedPO, renamed);
    PolicyEntity replacement =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            original.name(),
            AUDIT_INFO);

    CountDownLatch renameWritten = new CountDownLatch(1);
    CountDownLatch allowRenameCommit = new CountDownLatch(1);
    CountDownLatch overwriteStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> renameResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () ->
                        assertEquals(
                            Integer.valueOf(1),
                            SessionUtils.getWithoutCommit(
                                PolicyMetaMapper.class,
                                mapper -> mapper.updatePolicyMeta(renamedPO, observedPO))),
                    () ->
                        SessionUtils.doWithoutCommit(
                            PolicyVersionMapper.class,
                            mapper -> mapper.insertPolicyVersion(renamedPO.getPolicyVersionPO())),
                    () -> {
                      renameWritten.countDown();
                      await(allowRenameCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      assertTrue(renameWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> overwriteResult =
          executor.submit(
              () -> {
                overwriteStarted.countDown();
                try {
                  policyMetaService.insertPolicy(replacement, true);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(overwriteStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> overwriteResult.get(500, TimeUnit.MILLISECONDS));

      allowRenameCommit.countDown();
      Assertions.assertNull(renameResult.get(30, TimeUnit.SECONDS));
      Assertions.assertNull(overwriteResult.get(30, TimeUnit.SECONDS));
    } finally {
      allowRenameCommit.countDown();
      executor.shutdownNow();
    }

    assertEquals(
        original.id(), policyMetaService.getPolicyByIdentifier(renamed.nameIdentifier()).id());
    assertEquals(
        replacement.id(), policyMetaService.getPolicyByIdentifier(original.nameIdentifier()).id());
  }

  @TestTemplate
  public void testPolicyDeleteReturnsFalseWhenConcurrentDeleteWins() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy_concurrent_delete",
            AUDIT_INFO);
    policyMetaService.insertPolicy(policy, false);
    PolicyPO observedPO = getPolicyPO(policy.nameIdentifier());

    assertTrue(policyMetaService.deletePolicy(policy.nameIdentifier()));
    assertFalse(policyMetaService.deletePolicy(policy.nameIdentifier(), observedPO));
  }

  @TestTemplate
  public void testDeletePolicyCleansEveryDependentRelation() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE_NAME),
            "policy_cascade_occ",
            AUDIT_INFO);
    policyMetaService.insertPolicy(policy, false);
    TagEntity tag = createAndInsertTagEntity("tag_policy_cascade", "tag comment", METALAKE_NAME);
    backend.updateEntityRelations(
        RelationUpdate.of(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            new RelationEdgeTarget[] {
              RelationEdgeTarget.of(
                  policy.nameIdentifier(),
                  Entity.EntityType.POLICY,
                  "{\"type\":\"TAG_VALUE\",\"value\":\"finance\"}")
            },
            new RelationEdgeTarget[0]));
    TagMetaService.getInstance()
        .associateTagsWithMetadataObject(
            policy.nameIdentifier(),
            Entity.EntityType.POLICY,
            new NameIdentifier[] {tag.nameIdentifier()},
            new NameIdentifier[0]);

    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "user_policy_cascade",
            AUDIT_INFO);
    backend.insert(user, false);
    OwnerMetaService.getInstance()
        .setOwner(
            policy.nameIdentifier(), Entity.EntityType.POLICY, user.nameIdentifier(), user.type());

    RoleEntity role =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role_policy_cascade",
            AUDIT_INFO,
            Lists.newArrayList(
                SecurableObjects.ofPolicy(
                    policy.name(), Lists.newArrayList(Privileges.ApplyPolicy.allow()))),
            null);
    backend.insert(role, false);

    String policyAsMetadataObject =
        String.format("metadata_object_id = %d AND metadata_object_type = 'POLICY'", policy.id());
    assertEquals(1, countActiveRows("policy_tag_relation_meta", "policy_id = " + policy.id()));
    assertEquals(1, countActiveRows("tag_relation_meta", policyAsMetadataObject));
    assertEquals(1, countActiveRows("owner_meta", policyAsMetadataObject));
    assertEquals(
        1,
        countActiveRows(
            "role_meta_securable_object",
            String.format("metadata_object_id = %d AND type = 'POLICY'", policy.id())));

    assertTrue(policyMetaService.deletePolicy(policy.nameIdentifier()));

    assertEquals(0, countActiveRows("policy_tag_relation_meta", "policy_id = " + policy.id()));
    assertEquals(0, countActiveRows("tag_relation_meta", policyAsMetadataObject));
    assertEquals(0, countActiveRows("owner_meta", policyAsMetadataObject));
    assertEquals(
        0,
        countActiveRows(
            "role_meta_securable_object",
            String.format("metadata_object_id = %d AND type = 'POLICY'", policy.id())));
    assertEquals(0, listPolicyVersions(policy.id()).values().stream().filter(v -> v == 0L).count());
  }

  @TestTemplate
  public void testDeletePolicy() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policyEntity1 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE_NAME))
            .withComment("comment")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withAuditInfo(AUDIT_INFO)
            .build();
    policyMetaService.insertPolicy(policyEntity1, false);

    boolean deleted =
        policyMetaService.deletePolicy(NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1"));
    Assertions.assertTrue(deleted);

    deleted = policyMetaService.deletePolicy(NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1"));
    Assertions.assertFalse(deleted);

    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                policyMetaService.getPolicyByIdentifier(
                    NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1")));
    assertEquals("No such policy entity: policy1", excep.getMessage());
  }

  @TestTemplate
  public void testDeleteMetalake() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policyEntity1 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE_NAME))
            .withComment("comment")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withAuditInfo(AUDIT_INFO)
            .build();
    policyMetaService.insertPolicy(policyEntity1, false);

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            policyMetaService.getPolicyByIdentifier(
                NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1")));

    // Test delete metalake with cascade.
    BaseMetalake metalake1 =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME + "1", AUDIT_INFO);
    backend.insert(metalake1, false);

    PolicyEntity policyEntity2 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy2")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE_NAME + "1"))
            .withComment("comment")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withAuditInfo(AUDIT_INFO)
            .build();

    policyMetaService.insertPolicy(policyEntity2, false);
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake1.nameIdentifier(), true));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            policyMetaService.getPolicyByIdentifier(
                NameIdentifierUtil.ofPolicy(METALAKE_NAME + "1", "policy2")));
  }

  private int countActiveRows(String table, String whereClause) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM %s WHERE %s AND deleted_at = 0", table, whereClause))) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new RuntimeException("Doesn't contain data");
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  private PolicyPO getPolicyPO(NameIdentifier identifier) {
    return SessionUtils.getWithoutCommit(
        PolicyMetaMapper.class,
        mapper ->
            mapper.selectPolicyMetaByMetalakeAndName(
                identifier.namespace().level(0), identifier.name()));
  }

  /** Copies the policy under a new name and comment, keeping the audit info tests create with. */
  private PolicyEntity copyPolicy(PolicyEntity policy, String name, String comment) {
    return copyPolicy(policy, name, comment, AUDIT_INFO);
  }

  private PolicyEntity copyPolicy(
      PolicyEntity policy, String name, String comment, AuditInfo auditInfo) {
    return PolicyEntity.builder()
        .withId(policy.id())
        .withName(name)
        .withNamespace(policy.namespace())
        .withPolicyType(policy.policyType())
        .withComment(comment)
        .withEnabled(policy.enabled())
        .withContent(policy.content())
        .withAuditInfo(auditInfo)
        .build();
  }

  private void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
