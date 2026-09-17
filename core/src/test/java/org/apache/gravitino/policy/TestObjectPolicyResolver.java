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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.tag.EffectiveTagResolver;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestObjectPolicyResolver {

  private static final String METALAKE = "metalake";
  private static final MetadataObject OBJECT =
      MetadataObjects.of(Arrays.asList("catalog", "schema", "table"), MetadataObject.Type.TABLE);

  private EntityStore entityStore;
  private SupportsRelationOperations relationOperations;
  private EffectiveTagResolver effectiveTagResolver;
  private ObjectPolicyResolver resolver;

  @BeforeEach
  public void setUp() {
    entityStore = mock(EntityStore.class);
    relationOperations = mock(SupportsRelationOperations.class);
    effectiveTagResolver = mock(EffectiveTagResolver.class);
    when(entityStore.relationOperations()).thenReturn(relationOperations);
    resolver = new ObjectPolicyResolver(entityStore, effectiveTagResolver);
  }

  @Test
  public void testResolveByPresenceAndValueSelector() throws Exception {
    TagEntity domain = tag(1L, "domain", TagAssignment.ofValues("finance"));
    TagEntity classified = tag(2L, "classified", TagAssignment.noValue());
    PolicyEntity selected = policy(10L, "selected", true);
    PolicyEntity disabled = policy(11L, "disabled", false);
    when(effectiveTagResolver.resolve(METALAKE, OBJECT))
        .thenReturn(new TagEntity[] {domain, classified});
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Arrays.asList(
                NameIdentifierUtil.ofTag(METALAKE, "domain"),
                NameIdentifierUtil.ofTag(METALAKE, "classified")),
            Entity.EntityType.TAG))
        .thenReturn(
            Arrays.asList(
                relation(domain, selected, TagValueSelector.of("finance")),
                relation(classified, selected, AllValuesSelector.get()),
                relation(classified, disabled, AllValuesSelector.get())));

    PolicyEntity[] policies = resolver.resolve(METALAKE, OBJECT);

    Assertions.assertArrayEquals(new PolicyEntity[] {selected}, policies);
  }

  @Test
  public void testResolveMissingSelectorAsAllValues() throws Exception {
    TagEntity domain = tag(1L, "domain", TagAssignment.ofValues("finance"));
    PolicyEntity policy = policy(10L, "policy", true);
    when(effectiveTagResolver.resolve(METALAKE, OBJECT)).thenReturn(new TagEntity[] {domain});
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(NameIdentifierUtil.ofTag(METALAKE, "domain")),
            Entity.EntityType.TAG))
        .thenReturn(
            Collections.singletonList(
                new RelationalEntity<>(
                    SupportsRelationOperations.Type.POLICY_TAG_REL,
                    domain.nameIdentifier(),
                    Entity.EntityType.TAG,
                    policy,
                    null)));

    Assertions.assertArrayEquals(new PolicyEntity[] {policy}, resolver.resolve(METALAKE, OBJECT));
  }

  @Test
  public void testDropNonMatchingSelector() throws Exception {
    TagEntity domain = tag(1L, "domain", TagAssignment.ofValues("engineering"));
    PolicyEntity policy = policy(10L, "policy", true);
    when(effectiveTagResolver.resolve(METALAKE, OBJECT)).thenReturn(new TagEntity[] {domain});
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(NameIdentifierUtil.ofTag(METALAKE, "domain")),
            Entity.EntityType.TAG))
        .thenReturn(
            Collections.singletonList(relation(domain, policy, TagValueSelector.of("finance"))));

    Assertions.assertEquals(0, resolver.resolve(METALAKE, OBJECT).length);
  }

  @Test
  public void testRejectMixedSelectorResults() throws Exception {
    TagEntity domain = tag(1L, "domain", TagAssignment.ofValues("finance"));
    TagEntity classified = tag(2L, "classified", TagAssignment.ofValues("public"));
    PolicyEntity policy = policy(10L, "policy", true);
    when(effectiveTagResolver.resolve(METALAKE, OBJECT))
        .thenReturn(new TagEntity[] {domain, classified});
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Arrays.asList(
                NameIdentifierUtil.ofTag(METALAKE, "domain"),
                NameIdentifierUtil.ofTag(METALAKE, "classified")),
            Entity.EntityType.TAG))
        .thenReturn(
            Arrays.asList(
                relation(domain, policy, TagValueSelector.of("finance")),
                relation(classified, policy, TagValueSelector.of("pii"))));

    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class, () -> resolver.resolve(METALAKE, OBJECT));
    Assertions.assertTrue(exception.getMessage().contains("conflicting selector results"));
  }

  private static RelationalEntity<PolicyEntity> relation(
      TagEntity tag, PolicyEntity policy, PolicyAssociationSelector selector) {
    return new RelationalEntity<>(
        SupportsRelationOperations.Type.POLICY_TAG_REL,
        tag.nameIdentifier(),
        Entity.EntityType.TAG,
        policy,
        PolicyAssociationSelectorSerde.serialize(selector));
  }

  private static TagEntity tag(long id, String name, TagAssignment assignment) {
    return TagEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofTag(METALAKE))
        .withProperties(Collections.emptyMap())
        .withAuditInfo(audit())
        .build()
        .copyWithAssignment(assignment);
  }

  private static PolicyEntity policy(long id, String name, boolean enabled) {
    return PolicyEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
        .withPolicyType(Policy.BuiltInType.CUSTOM)
        .withEnabled(enabled)
        .withContent(
            PolicyContents.custom(
                ImmutableMap.of("rule", "value"), ImmutableSet.of(MetadataObject.Type.TABLE), null))
        .withAuditInfo(audit())
        .build();
  }

  private static AuditInfo audit() {
    return AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build();
  }
}
