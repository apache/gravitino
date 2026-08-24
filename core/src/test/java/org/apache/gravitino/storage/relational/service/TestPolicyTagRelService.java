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
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/** Tests policy-to-tag relation persistence and lifecycle behavior. */
public class TestPolicyTagRelService extends TestJDBCBackend {

  private static final String METALAKE = "policy_tag_relation_metalake";
  private static final String FINANCE_SELECTOR = "{\"type\":\"TAG_VALUE\",\"value\":\"finance\"}";
  private static final String RISK_SELECTOR = "{\"type\":\"TAG_VALUE\",\"value\":\"risk\"}";

  @TestTemplate
  public void testSelectorUpsertBidirectionalReadAndIdempotentDelete() throws IOException {
    createAndInsertMakeLake(METALAKE);
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("domain")
            .withNamespace(NamespaceUtil.ofTag(METALAKE))
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE),
            "retention",
            AUDIT_INFO);
    backend.insert(tag, false);
    backend.insert(policy, false);

    RelationEdgeTarget financeTarget =
        RelationEdgeTarget.of(policy.nameIdentifier(), Entity.EntityType.POLICY, FINANCE_SELECTOR);
    RelationUpdate financeUpdate =
        RelationUpdate.of(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            new RelationEdgeTarget[] {financeTarget},
            new RelationEdgeTarget[0]);
    backend.updateEntityRelations(financeUpdate);
    backend.updateEntityRelations(financeUpdate);

    List<RelationalEntity<?>> byTag =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(tag.nameIdentifier()),
            Entity.EntityType.TAG);
    Assertions.assertEquals(1, byTag.size());
    Assertions.assertEquals(SupportsRelationOperations.Type.POLICY_TAG_REL, byTag.get(0).type());
    Assertions.assertEquals(tag.nameIdentifier(), byTag.get(0).source());
    Assertions.assertEquals(Entity.EntityType.TAG, byTag.get(0).sourceType());
    Assertions.assertEquals(policy, byTag.get(0).targetEntity());
    Assertions.assertEquals(FINANCE_SELECTOR, byTag.get(0).relationValue().orElse(null));
    Assertions.assertEquals(
        policy,
        backend.getEntityByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            policy.nameIdentifier()));

    RelationEdgeTarget riskTarget =
        RelationEdgeTarget.of(policy.nameIdentifier(), Entity.EntityType.POLICY, RISK_SELECTOR);
    backend.updateEntityRelations(
        RelationUpdate.of(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            new RelationEdgeTarget[] {riskTarget},
            new RelationEdgeTarget[0]));
    List<RelationalEntity<?>> byPolicy =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(policy.nameIdentifier()),
            Entity.EntityType.POLICY);
    Assertions.assertEquals(1, byPolicy.size());
    Assertions.assertEquals(policy.nameIdentifier(), byPolicy.get(0).source());
    Assertions.assertEquals(Entity.EntityType.POLICY, byPolicy.get(0).sourceType());
    Assertions.assertEquals(tag, byPolicy.get(0).targetEntity());
    Assertions.assertEquals(RISK_SELECTOR, byPolicy.get(0).relationValue().orElse(null));

    RelationUpdate removeUpdate =
        RelationUpdate.of(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            new RelationEdgeTarget[0],
            new RelationEdgeTarget[] {riskTarget});
    backend.updateEntityRelations(removeUpdate);
    backend.updateEntityRelations(removeUpdate);
    Assertions.assertTrue(
        backend
            .batchListEntitiesByRelation(
                SupportsRelationOperations.Type.POLICY_TAG_REL,
                Collections.singletonList(NameIdentifierUtil.ofTag(METALAKE, tag.name())),
                Entity.EntityType.TAG)
            .isEmpty());
  }

  @TestTemplate
  public void testEntityDeletesCascadePolicyTagRelations() throws IOException {
    createAndInsertMakeLake(METALAKE);
    TagEntity policyDeletedTag =
        createAssociation(METALAKE, "policy_deleted_tag", "deleted_policy");
    backend.delete(
        NameIdentifierUtil.ofPolicy(METALAKE, "deleted_policy"), Entity.EntityType.POLICY, false);
    Assertions.assertTrue(
        backend
            .batchListEntitiesByRelation(
                SupportsRelationOperations.Type.POLICY_TAG_REL,
                Collections.singletonList(policyDeletedTag.nameIdentifier()),
                Entity.EntityType.TAG)
            .isEmpty());

    TagEntity deletedTag = createAssociation(METALAKE, "deleted_tag", "surviving_policy");
    backend.delete(deletedTag.nameIdentifier(), Entity.EntityType.TAG, false);
    Assertions.assertTrue(
        backend
            .batchListEntitiesByRelation(
                SupportsRelationOperations.Type.POLICY_TAG_REL,
                Collections.singletonList(
                    NameIdentifierUtil.ofPolicy(METALAKE, "surviving_policy")),
                Entity.EntityType.POLICY)
            .isEmpty());
  }

  @TestTemplate
  public void testMetalakeCascadeDoesNotDeleteRelationsFromOtherMetalakes() throws IOException {
    BaseMetalake deletedMetalake = createAndInsertMakeLake(METALAKE);
    createAssociation(METALAKE, "deleted_domain", "deleted_retention");
    String survivingMetalake = METALAKE + "_surviving";
    createAndInsertMakeLake(survivingMetalake);
    TagEntity survivingTag =
        createAssociation(survivingMetalake, "surviving_domain", "surviving_retention");

    backend.delete(deletedMetalake.nameIdentifier(), Entity.EntityType.METALAKE, true);

    List<RelationalEntity<?>> survivingRelations =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(survivingTag.nameIdentifier()),
            Entity.EntityType.TAG);
    Assertions.assertEquals(1, survivingRelations.size());
    Assertions.assertEquals("surviving_retention", survivingRelations.get(0).targetEntity().name());
  }

  private TagEntity createAssociation(String metalake, String tagName, String policyName)
      throws IOException {
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(tagName)
            .withNamespace(NamespaceUtil.ofTag(metalake))
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(metalake),
            policyName,
            AUDIT_INFO);
    backend.insert(tag, false);
    backend.insert(policy, false);
    backend.updateEntityRelations(
        RelationUpdate.of(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            new RelationEdgeTarget[] {
              RelationEdgeTarget.of(policy.nameIdentifier(), Entity.EntityType.POLICY, null)
            },
            new RelationEdgeTarget[0]));
    return tag;
  }
}
