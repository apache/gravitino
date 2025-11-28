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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
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
    assertEquals(3, listPolicyVersions(anotherPolicy.id()).size());

    // soft delete for old version policy
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.deleteOldVersionData(entityType, 1);
    }
    Map<Integer, Long> versionDeletedMap2 = listPolicyVersions(anotherPolicy.id());
    assertEquals(3, versionDeletedMap2.size());
    assertEquals(1, versionDeletedMap2.values().stream().filter(value -> value == 0L).count());
    assertEquals(2, versionDeletedMap2.values().stream().filter(value -> value != 0L).count());
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

  @TestTemplate
  public void testAssociateAndDisassociatePoliciesWithMetadataObject() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME),
            "catalog1",
            AUDIT_INFO);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name()),
            "schema1",
            AUDIT_INFO);
    backend.insert(schema, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "table1",
            AUDIT_INFO);
    backend.insert(table, false);

    // Create policies to associate
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

    PolicyEntity policyEntity3 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy3")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE_NAME))
            .withComment("comment")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withContent(content)
            .withAuditInfo(AUDIT_INFO)
            .build();
    policyMetaService.insertPolicy(policyEntity3, false);

    // Test associate policies with metadata object
    NameIdentifier[] policiesToAdd =
        new NameIdentifier[] {
          NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1"),
          NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy2"),
          NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy3")
        };

    List<PolicyEntity> policyEntities =
        policyMetaService.associatePoliciesWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), policiesToAdd, new NameIdentifier[0]);
    assertEquals(3, policyEntities.size());
    Assertions.assertTrue(policyEntities.contains(policyEntity1));
    Assertions.assertTrue(policyEntities.contains(policyEntity2));
    Assertions.assertTrue(policyEntities.contains(policyEntity3));

    // Test disassociate policies with metadata object
    NameIdentifier[] policiesToRemove =
        new NameIdentifier[] {NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1")};

    List<PolicyEntity> policyEntities1 =
        policyMetaService.associatePoliciesWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), new NameIdentifier[0], policiesToRemove);

    assertEquals(2, policyEntities1.size());
    Assertions.assertFalse(policyEntities1.contains(policyEntity1));
    Assertions.assertTrue(policyEntities1.contains(policyEntity2));
    Assertions.assertTrue(policyEntities1.contains(policyEntity3));

    // Test no policies to associate and disassociate
    List<PolicyEntity> policyEntities2 =
        policyMetaService.associatePoliciesWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), new NameIdentifier[0], new NameIdentifier[0]);
    assertEquals(2, policyEntities2.size());
    Assertions.assertFalse(policyEntities2.contains(policyEntity1));
    Assertions.assertTrue(policyEntities2.contains(policyEntity2));
    Assertions.assertTrue(policyEntities2.contains(policyEntity3));

    // Test associate and disassociate same policies with metadata object
    List<PolicyEntity> policyEntities3 =
        policyMetaService.associatePoliciesWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), policiesToRemove, policiesToRemove);

    assertEquals(2, policyEntities3.size());
    Assertions.assertFalse(policyEntities3.contains(policyEntity1));
    Assertions.assertTrue(policyEntities3.contains(policyEntity2));
    Assertions.assertTrue(policyEntities3.contains(policyEntity3));

    // Test associate and disassociate in-existent policies with metadata object
    NameIdentifier[] policiesToAdd1 =
        new NameIdentifier[] {
          NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy4"),
          NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy5")
        };

    NameIdentifier[] policiesToRemove1 =
        new NameIdentifier[] {
          NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy6"),
          NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy7")
        };

    List<PolicyEntity> policyEntities4 =
        policyMetaService.associatePoliciesWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), policiesToAdd1, policiesToRemove1);

    assertEquals(2, policyEntities4.size());
    Assertions.assertTrue(policyEntities4.contains(policyEntity2));
    Assertions.assertTrue(policyEntities4.contains(policyEntity3));

    // Test associate already associated policies with metadata object
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            policyMetaService.associatePoliciesWithMetadataObject(
                catalog.nameIdentifier(), catalog.type(), policiesToAdd, new NameIdentifier[0]));

    // Test disassociate already disassociated policies with metadata object
    List<PolicyEntity> policyEntities5 =
        policyMetaService.associatePoliciesWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), new NameIdentifier[0], policiesToRemove);

    assertEquals(2, policyEntities5.size());
    Assertions.assertTrue(policyEntities5.contains(policyEntity2));
    Assertions.assertTrue(policyEntities5.contains(policyEntity3));

    // Test associate and disassociate with invalid metadata object
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            policyMetaService.associatePoliciesWithMetadataObject(
                NameIdentifier.of(METALAKE_NAME, "non-existent-catalog"),
                catalog.type(),
                policiesToAdd,
                policiesToRemove));

    // Test associate and disassociate to a schema
    List<PolicyEntity> policyEntities6 =
        policyMetaService.associatePoliciesWithMetadataObject(
            schema.nameIdentifier(), schema.type(), policiesToAdd, policiesToRemove);

    assertEquals(2, policyEntities6.size());
    Assertions.assertTrue(policyEntities6.contains(policyEntity2));
    Assertions.assertTrue(policyEntities6.contains(policyEntity3));

    // Test associate and disassociate to a table
    List<PolicyEntity> policyEntities7 =
        policyMetaService.associatePoliciesWithMetadataObject(
            table.nameIdentifier(), table.type(), policiesToAdd, policiesToRemove);

    assertEquals(2, policyEntities7.size());
    Assertions.assertTrue(policyEntities7.contains(policyEntity2));
    Assertions.assertTrue(policyEntities7.contains(policyEntity3));
  }

  @TestTemplate
  public void testListPoliciesForMetadataObject() throws IOException {
    testAssociateAndDisassociatePoliciesWithMetadataObject();

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();

    // Test list policies for catalog
    List<PolicyEntity> policyEntities =
        policyMetaService.listPoliciesForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1"), Entity.EntityType.CATALOG);
    assertEquals(2, policyEntities.size());
    Assertions.assertTrue(
        policyEntities.stream().anyMatch(policyEntity -> policyEntity.name().equals("policy2")));
    Assertions.assertTrue(
        policyEntities.stream().anyMatch(policyEntity -> policyEntity.name().equals("policy3")));

    // Test list policies for schema
    List<PolicyEntity> policyEntities1 =
        policyMetaService.listPoliciesForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1"), Entity.EntityType.SCHEMA);

    assertEquals(2, policyEntities1.size());
    Assertions.assertTrue(
        policyEntities1.stream().anyMatch(policyEntity -> policyEntity.name().equals("policy2")));
    Assertions.assertTrue(
        policyEntities1.stream().anyMatch(policyEntity -> policyEntity.name().equals("policy3")));

    // Test list policies for table
    List<PolicyEntity> policyEntities2 =
        policyMetaService.listPoliciesForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table1"),
            Entity.EntityType.TABLE);

    assertEquals(2, policyEntities2.size());
    Assertions.assertTrue(
        policyEntities2.stream().anyMatch(policyEntity -> policyEntity.name().equals("policy2")));
    Assertions.assertTrue(
        policyEntities2.stream().anyMatch(policyEntity -> policyEntity.name().equals("policy3")));

    // Test list policies for non-existent metadata object
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            policyMetaService.listPoliciesForMetadataObject(
                NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table2"),
                Entity.EntityType.TABLE));
  }

  @TestTemplate
  public void testGetPolicyForMetadataObject() throws IOException {
    testAssociateAndDisassociatePoliciesWithMetadataObject();

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();

    // Test get policy for catalog
    PolicyEntity policyEntity =
        policyMetaService.getPolicyForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1"),
            Entity.EntityType.CATALOG,
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy2"));
    assertEquals("policy2", policyEntity.name());

    // Test get policy for schema
    PolicyEntity policyEntity1 =
        policyMetaService.getPolicyForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1"),
            Entity.EntityType.SCHEMA,
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy3"));
    assertEquals("policy3", policyEntity1.name());

    // Test get policy for table
    PolicyEntity policyEntity2 =
        policyMetaService.getPolicyForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table1"),
            Entity.EntityType.TABLE,
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy2"));
    assertEquals("policy2", policyEntity2.name());

    // Test get policy for non-existent metadata object
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            policyMetaService.getPolicyForMetadataObject(
                NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table2"),
                Entity.EntityType.TABLE,
                NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy2")));

    // Test get policy for non-existent policy
    Throwable e =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                policyMetaService.getPolicyForMetadataObject(
                    NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table1"),
                    Entity.EntityType.TABLE,
                    NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy4")));
    Assertions.assertTrue(e.getMessage().contains("No such policy entity: policy4"));
  }

  @TestTemplate
  public void testListAssociatedEntitiesForPolicy() throws IOException {
    testAssociateAndDisassociatePoliciesWithMetadataObject();

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();

    // Test list associated dummy entities for policy2
    List<GenericEntity> entities =
        policyMetaService.listAssociatedEntitiesForPolicy(
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy2"));

    assertEquals(3, entities.size());
    Set<Entity.EntityType> actualTypes =
        entities.stream().map(GenericEntity::type).collect(Collectors.toSet());
    Assertions.assertTrue(actualTypes.contains(Entity.EntityType.CATALOG));
    Assertions.assertTrue(actualTypes.contains(Entity.EntityType.SCHEMA));
    Assertions.assertTrue(actualTypes.contains(Entity.EntityType.TABLE));

    // Test list associated dummy entities for policy3
    List<GenericEntity> entities1 =
        policyMetaService.listAssociatedEntitiesForPolicy(
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy3"));

    assertEquals(3, entities1.size());
    Set<Entity.EntityType> actualTypes1 =
        entities1.stream().map(GenericEntity::type).collect(Collectors.toSet());
    Assertions.assertTrue(actualTypes1.contains(Entity.EntityType.CATALOG));
    Assertions.assertTrue(actualTypes1.contains(Entity.EntityType.SCHEMA));
    Assertions.assertTrue(actualTypes1.contains(Entity.EntityType.TABLE));

    // Test list associated dummy entities for non-existent policy
    List<GenericEntity> entities2 =
        policyMetaService.listAssociatedEntitiesForPolicy(
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy4"));
    assertEquals(0, entities2.size());

    // Test metadata object non-exist scenario.
    backend.delete(
        NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table1"),
        Entity.EntityType.TABLE,
        false);

    List<GenericEntity> entities3 =
        policyMetaService.listAssociatedEntitiesForPolicy(
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy2"));

    assertEquals(2, entities3.size());
    Set<Entity.EntityType> actualTypes3 =
        entities3.stream().map(GenericEntity::type).collect(Collectors.toSet());
    Assertions.assertTrue(actualTypes3.contains(Entity.EntityType.CATALOG));
    Assertions.assertTrue(actualTypes3.contains(Entity.EntityType.SCHEMA));

    backend.delete(
        NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1"), Entity.EntityType.SCHEMA, false);

    List<GenericEntity> entities4 =
        policyMetaService.listAssociatedEntitiesForPolicy(
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy2"));

    assertEquals(1, entities4.size());
    Set<Entity.EntityType> actualTypes4 =
        entities4.stream().map(GenericEntity::type).collect(Collectors.toSet());
    Assertions.assertTrue(actualTypes4.contains(Entity.EntityType.CATALOG));

    backend.delete(NameIdentifier.of(METALAKE_NAME, "catalog1"), Entity.EntityType.CATALOG, false);

    List<GenericEntity> entities5 =
        policyMetaService.listAssociatedEntitiesForPolicy(
            NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy2"));

    assertEquals(0, entities5.size());
  }

  @TestTemplate
  public void testDeleteMetadataObjectForPolicy() throws IOException {
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

    // 1. Test non-cascade deletion
    EntitiesToTest entities = createAndAssociateEntities("catalog1", "schema1", policyEntity1);
    assertEquals(6, countActivePolicyRel(policyEntity1.id()));
    assertEquals(6, countAllPolicyRel(policyEntity1.id()));

    // Test to delete a model
    ModelMetaService.getInstance().deleteModel(entities.model.nameIdentifier());
    assertEquals(5, countActivePolicyRel(policyEntity1.id()));
    assertEquals(6, countAllPolicyRel(policyEntity1.id()));

    // Test to drop a table
    TableMetaService.getInstance().deleteTable(entities.table.nameIdentifier());
    assertEquals(4, countActivePolicyRel(policyEntity1.id()));
    assertEquals(6, countAllPolicyRel(policyEntity1.id()));

    // Test to drop a topic
    TopicMetaService.getInstance().deleteTopic(entities.topic.nameIdentifier());
    assertEquals(3, countActivePolicyRel(policyEntity1.id()));
    assertEquals(6, countAllPolicyRel(policyEntity1.id()));

    // Test to drop a fileset
    FilesetMetaService.getInstance().deleteFileset(entities.fileset.nameIdentifier());
    assertEquals(2, countActivePolicyRel(policyEntity1.id()));
    assertEquals(6, countAllPolicyRel(policyEntity1.id()));

    // Test to drop a schema
    SchemaMetaService.getInstance().deleteSchema(entities.schema.nameIdentifier(), false);
    assertEquals(1, countActivePolicyRel(policyEntity1.id()));
    assertEquals(6, countAllPolicyRel(policyEntity1.id()));

    // Test to drop a catalog
    CatalogMetaService.getInstance().deleteCatalog(entities.catalog.nameIdentifier(), false);
    assertEquals(0, countActivePolicyRel(policyEntity1.id()));
    assertEquals(6, countAllPolicyRel(policyEntity1.id()));

    // 2. Test cascade deletion for catalog
    EntitiesToTest entitiesForCascadeCatalog =
        createAndAssociateEntities("catalog2", "schema2", policyEntity1);
    CatalogMetaService.getInstance()
        .deleteCatalog(entitiesForCascadeCatalog.catalog.nameIdentifier(), true);
    assertEquals(0, countActivePolicyRel(policyEntity1.id()));
    // 6 from previous test + 6 from this test
    assertEquals(12, countAllPolicyRel(policyEntity1.id()));

    // 3. Test cascade deletion for schema
    EntitiesToTest entitiesForCascadeSchema =
        createAndAssociateEntities("catalog3", "schema3", policyEntity1);
    SchemaMetaService.getInstance()
        .deleteSchema(entitiesForCascadeSchema.schema.nameIdentifier(), true);
    assertEquals(1, countActivePolicyRel(policyEntity1.id()));
    // 12 from previous tests + 6 from this test
    assertEquals(18, countAllPolicyRel(policyEntity1.id()));
  }

  private static class EntitiesToTest {
    final CatalogEntity catalog;
    final SchemaEntity schema;
    final TableEntity table;
    final TopicEntity topic;
    final FilesetEntity fileset;
    final ModelEntity model;

    EntitiesToTest(
        CatalogEntity catalog,
        SchemaEntity schema,
        TableEntity table,
        TopicEntity topic,
        FilesetEntity fileset,
        ModelEntity model) {
      this.catalog = catalog;
      this.schema = schema;
      this.table = table;
      this.topic = topic;
      this.fileset = fileset;
      this.model = model;
    }
  }

  private EntitiesToTest createAndAssociateEntities(
      String catalogName, String schemaName, PolicyEntity policyEntity) throws IOException {
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    NameIdentifier policyIdent = policyEntity.nameIdentifier();

    // Create entities
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME),
            catalogName,
            AUDIT_INFO);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name()),
            schemaName,
            AUDIT_INFO);
    backend.insert(schema, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "table1",
            AUDIT_INFO);
    backend.insert(table, false);

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "topic1",
            AUDIT_INFO);
    backend.insert(topic, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "fileset1",
            AUDIT_INFO);
    backend.insert(fileset, false);

    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "model1",
            "comment",
            1,
            null,
            AUDIT_INFO);
    backend.insert(model, false);

    // Associate policy with all entities
    policyMetaService.associatePoliciesWithMetadataObject(
        catalog.nameIdentifier(),
        catalog.type(),
        new NameIdentifier[] {policyIdent},
        new NameIdentifier[0]);
    policyMetaService.associatePoliciesWithMetadataObject(
        schema.nameIdentifier(),
        schema.type(),
        new NameIdentifier[] {policyIdent},
        new NameIdentifier[0]);
    policyMetaService.associatePoliciesWithMetadataObject(
        table.nameIdentifier(),
        table.type(),
        new NameIdentifier[] {policyIdent},
        new NameIdentifier[0]);
    policyMetaService.associatePoliciesWithMetadataObject(
        topic.nameIdentifier(),
        topic.type(),
        new NameIdentifier[] {policyIdent},
        new NameIdentifier[0]);
    policyMetaService.associatePoliciesWithMetadataObject(
        fileset.nameIdentifier(),
        fileset.type(),
        new NameIdentifier[] {policyIdent},
        new NameIdentifier[0]);
    policyMetaService.associatePoliciesWithMetadataObject(
        model.nameIdentifier(),
        model.type(),
        new NameIdentifier[] {policyIdent},
        new NameIdentifier[0]);

    return new EntitiesToTest(catalog, schema, table, topic, fileset, model);
  }

  private Integer countActivePolicyRel(Long policyId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM policy_relation_meta WHERE policy_id = %d AND deleted_at = 0",
                    policyId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  private Integer countAllPolicyRel(Long policyId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM policy_relation_meta WHERE policy_id = %d", policyId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }
}
