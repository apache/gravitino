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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.CustomPolicy;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyMetaService extends TestJDBCBackend {
  private final String metalakeName = "metalake_for_policy_test";

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  private final CustomPolicy.CustomContent content =
      CustomPolicy.contentBuilder().withCustomFields(ImmutableMap.of("filed1", 123)).build();

  private final Set<MetadataObject.Type> supportedObjectTypes =
      new HashSet<MetadataObject.Type>() {
        {
          add(MetadataObject.Type.CATALOG);
          add(MetadataObject.Type.SCHEMA);
        }
      };

  @Test
  public void testInsertAndGetPolicyByIdentifier() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    // Test no policy entity.
    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                policyMetaService.getPolicyByIdentifier(
                    NameIdentifierUtil.ofPolicy(metalakeName, "policy1")));
    Assertions.assertEquals("No such policy entity: policy1", excep.getMessage());

    // Test get policy entity
    PolicyEntity policyEntity =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName))
            .withComment("comment")
            .withType("test")
            .withContent(content)
            .withEnabled(true)
            .withSupportedObjectTypes(supportedObjectTypes)
            .withAuditInfo(auditInfo)
            .build();
    policyMetaService.insertPolicy(policyEntity, false);

    PolicyEntity resultpolicyEntity =
        policyMetaService.getPolicyByIdentifier(
            NameIdentifierUtil.ofPolicy(metalakeName, "policy1"));
    Assertions.assertEquals(policyEntity, resultpolicyEntity);

    // Test with null comment and content properties.
    PolicyEntity PolicyEntity1 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy2")
            .withType("test")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName))
            .withAuditInfo(auditInfo)
            .withSupportedObjectTypes(supportedObjectTypes)
            .withContent(content)
            .build();

    policyMetaService.insertPolicy(PolicyEntity1, false);
    PolicyEntity resultPolicyEntity1 =
        policyMetaService.getPolicyByIdentifier(
            NameIdentifierUtil.ofPolicy(metalakeName, "policy2"));
    Assertions.assertEquals(PolicyEntity1, resultPolicyEntity1);
    Assertions.assertTrue(resultPolicyEntity1.enabled());
    Assertions.assertNull(resultPolicyEntity1.comment());
    Assertions.assertEquals(supportedObjectTypes, resultPolicyEntity1.supportedObjectTypes());
    Assertions.assertNull(resultPolicyEntity1.content().properties());

    // Test insert with overwrite.
    PolicyEntity PolicyEntity2 =
        PolicyEntity.builder()
            .withId(PolicyEntity1.id())
            .withName("policy3")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName))
            .withComment("comment")
            .withType("test")
            .withSupportedObjectTypes(supportedObjectTypes)
            .withContent(content)
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertThrows(
        Exception.class, () -> policyMetaService.insertPolicy(PolicyEntity2, false));

    policyMetaService.insertPolicy(PolicyEntity2, true);

    PolicyEntity resultPolicyEntity2 =
        policyMetaService.getPolicyByIdentifier(
            NameIdentifierUtil.ofPolicy(metalakeName, "policy3"));
    Assertions.assertEquals(PolicyEntity2, resultPolicyEntity2);
  }

  @Test
  public void testCreateAndListPolicies() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policyEntity1 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName))
            .withComment("comment")
            .withType("test")
            .withContent(content)
            .withSupportedObjectTypes(supportedObjectTypes)
            .withAuditInfo(auditInfo)
            .build();
    policyMetaService.insertPolicy(policyEntity1, false);

    PolicyEntity policyEntity2 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy2")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName))
            .withComment("comment")
            .withType("test")
            .withContent(content)
            .withSupportedObjectTypes(supportedObjectTypes)
            .withAuditInfo(auditInfo)
            .build();
    policyMetaService.insertPolicy(policyEntity2, false);

    List<PolicyEntity> policyEntities =
        policyMetaService.listPoliciesByNamespace(NamespaceUtil.ofPolicy(metalakeName));
    Assertions.assertEquals(2, policyEntities.size());
    Assertions.assertTrue(policyEntities.contains(policyEntity1));
    Assertions.assertTrue(policyEntities.contains(policyEntity2));
  }

  @Test
  public void testUpdatePolicy() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policyEntity1 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName))
            .withComment("comment")
            .withType("test")
            .withSupportedObjectTypes(supportedObjectTypes)
            .withContent(content)
            .withAuditInfo(auditInfo)
            .build();
    policyMetaService.insertPolicy(policyEntity1, false);

    // Update with no policy entity.
    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                policyMetaService.updatePolicy(
                    NameIdentifierUtil.ofPolicy(metalakeName, "policy2"),
                    policyEntity -> policyEntity));
    Assertions.assertEquals("No such policy entity: policy2", excep.getMessage());

    // Update policy entity.
    PolicyEntity policyEntity2 =
        PolicyEntity.builder()
            .withId(policyEntity1.id())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName))
            .withComment("comment1")
            .withType("test")
            .withSupportedObjectTypes(supportedObjectTypes)
            .withContent(content)
            .withAuditInfo(auditInfo)
            .build();
    PolicyEntity updatedPolicyEntity =
        policyMetaService.updatePolicy(
            NameIdentifierUtil.ofPolicy(metalakeName, "policy1"), policyEntity -> policyEntity2);
    Assertions.assertEquals(policyEntity2, updatedPolicyEntity);

    PolicyEntity loadedPolicyEntity =
        policyMetaService.getPolicyByIdentifier(
            NameIdentifierUtil.ofPolicy(metalakeName, "policy1"));
    Assertions.assertEquals(policyEntity2, loadedPolicyEntity);

    // Update with different id.
    PolicyEntity policyEntity3 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName))
            .withComment("comment1")
            .withType("test")
            .withSupportedObjectTypes(supportedObjectTypes)
            .withContent(content)
            .withAuditInfo(auditInfo)
            .build();

    Exception excep1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                policyMetaService.updatePolicy(
                    NameIdentifierUtil.ofPolicy(metalakeName, "policy1"),
                    policyEntity -> policyEntity3));
    Assertions.assertEquals(
        "The updated policy entity id: "
            + policyEntity3.id()
            + " must have the same id as the old "
            + "entity id "
            + policyEntity2.id(),
        excep1.getMessage());

    PolicyEntity loadedPolicyEntity1 =
        policyMetaService.getPolicyByIdentifier(
            NameIdentifierUtil.ofPolicy(metalakeName, "policy1"));
    Assertions.assertEquals(policyEntity2, loadedPolicyEntity1);
  }

  @Test
  public void testDeletePolicy() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policyEntity1 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName))
            .withComment("comment")
            .withType("test")
            .withSupportedObjectTypes(supportedObjectTypes)
            .withContent(content)
            .withAuditInfo(auditInfo)
            .build();
    policyMetaService.insertPolicy(policyEntity1, false);

    boolean deleted =
        policyMetaService.deletePolicy(NameIdentifierUtil.ofPolicy(metalakeName, "policy1"));
    Assertions.assertTrue(deleted);

    deleted = policyMetaService.deletePolicy(NameIdentifierUtil.ofPolicy(metalakeName, "policy1"));
    Assertions.assertFalse(deleted);

    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                policyMetaService.getPolicyByIdentifier(
                    NameIdentifierUtil.ofPolicy(metalakeName, "policy1")));
    Assertions.assertEquals("No such policy entity: policy1", excep.getMessage());
  }

  @Test
  public void testDeleteMetalake() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    PolicyMetaService policyMetaService = PolicyMetaService.getInstance();
    PolicyEntity policyEntity1 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy1")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName))
            .withComment("comment")
            .withType("test")
            .withSupportedObjectTypes(supportedObjectTypes)
            .withContent(content)
            .withAuditInfo(auditInfo)
            .build();
    policyMetaService.insertPolicy(policyEntity1, false);

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            policyMetaService.getPolicyByIdentifier(
                NameIdentifierUtil.ofPolicy(metalakeName, "policy1")));

    // Test delete metalake with cascade.
    BaseMetalake metalake1 =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName + "1", auditInfo);
    backend.insert(metalake1, false);

    PolicyEntity policyEntity2 =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("policy2")
            .withNamespace(NamespaceUtil.ofPolicy(metalakeName + "1"))
            .withComment("comment")
            .withType("test")
            .withSupportedObjectTypes(supportedObjectTypes)
            .withContent(content)
            .withAuditInfo(auditInfo)
            .build();

    policyMetaService.insertPolicy(policyEntity2, false);
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake1.nameIdentifier(), true));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            policyMetaService.getPolicyByIdentifier(
                NameIdentifierUtil.ofPolicy(metalakeName + "1", "policy2")));
  }
}
