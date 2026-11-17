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

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.ResolvedTagPolicy;
import org.apache.gravitino.authorization.TagPolicyMatcher;
import org.apache.gravitino.authorization.TagPolicyResolver;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.tag.EffectiveTagResolver;
import org.apache.gravitino.tag.EffectiveTagResolver.ResolvedTag;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestObjectPolicyResolver {

  private static final String METALAKE = "metalake";
  private static final MetadataObject OBJECT =
      MetadataObjects.of(Arrays.asList("catalog", "schema", "table"), MetadataObject.Type.TABLE);
  private static final MetadataObject CATALOG =
      MetadataObjects.of(Collections.singletonList("catalog"), MetadataObject.Type.CATALOG);

  private static final MetadataObject SCHEMA =
      MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA);

  private EntityStore entityStore;
  private SupportsRelationOperations relationOperations;
  private EffectiveTagResolver effectiveTagResolver;
  private ObjectPolicyResolver resolver;
  private TagEntity gold;
  private TagEntity bronze;
  private PolicyEntity access;

  @BeforeEach
  public void setUp() {
    entityStore = mock(EntityStore.class);
    relationOperations = mock(SupportsRelationOperations.class);
    effectiveTagResolver = mock(EffectiveTagResolver.class);
    when(entityStore.relationOperations()).thenReturn(relationOperations);
    resolver = new ObjectPolicyResolver(entityStore, effectiveTagResolver);
    gold = tag(1L, "certified", TagAssignment.ofValues("gold"));
    bronze = tag(1L, "certified", TagAssignment.ofValues("bronze"));
    access =
        PolicyEntity.builder()
            .withId(10L)
            .withName("policy_10")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
            .withPolicyType(Policy.BuiltInType.ACCESS_CONTROL)
            .withEnabled(true)
            .withContent(
                PolicyContents.accessControl(
                    List.of(Privilege.Name.SELECT_TABLE), List.of("analyst")))
            .withAuditInfo(audit())
            .build();
  }

  @Test
  public void testResolveByPresenceAndValueSelector() throws Exception {
    TagEntity domain =
        tag(1L, "domain", TagAssignment.ofValues("finance")).copyWithInherited(false);
    TagEntity classified = tag(2L, "classified", TagAssignment.noValue()).copyWithInherited(true);
    PolicyEntity selected = policy(10L, "selected", true);
    PolicyEntity disabled = policy(11L, "disabled", false);
    when(effectiveTagResolver.resolveWithSources(METALAKE, OBJECT))
        .thenReturn(
            new ResolvedTag[] {
              new ResolvedTag(domain, OBJECT), new ResolvedTag(classified, CATALOG)
            });
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
    Assertions.assertFalse(policies[0].inherited().orElseThrow());

    ObjectPolicyResolver.Match[] matches = resolver.resolveMatches(METALAKE, OBJECT);
    Assertions.assertEquals(1, matches.length);
    Assertions.assertEquals(List.of(OBJECT, CATALOG), matches[0].sources());
  }

  @Test
  public void testResolveMissingSelectorAsAllValues() throws Exception {
    TagEntity domain = tag(1L, "domain", TagAssignment.ofValues("finance")).copyWithInherited(true);
    PolicyEntity policy = policy(10L, "policy", true);
    when(effectiveTagResolver.resolveWithSources(METALAKE, OBJECT))
        .thenReturn(new ResolvedTag[] {new ResolvedTag(domain, CATALOG)});
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

    PolicyEntity[] policies = resolver.resolve(METALAKE, OBJECT);
    Assertions.assertArrayEquals(new PolicyEntity[] {policy}, policies);
    Assertions.assertTrue(policies[0].inherited().orElseThrow());
    Assertions.assertEquals(
        List.of(CATALOG), resolver.resolveMatches(METALAKE, OBJECT)[0].sources());
  }

  @Test
  public void testDropNonMatchingSelector() throws Exception {
    TagEntity domain = tag(1L, "domain", TagAssignment.ofValues("engineering"));
    PolicyEntity policy = policy(10L, "policy", true);
    when(effectiveTagResolver.resolveWithSources(METALAKE, OBJECT))
        .thenReturn(new ResolvedTag[] {new ResolvedTag(domain, OBJECT)});
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(NameIdentifierUtil.ofTag(METALAKE, "domain")),
            Entity.EntityType.TAG))
        .thenReturn(
            Collections.singletonList(relation(domain, policy, TagValueSelector.of("finance"))));

    Assertions.assertEquals(0, resolver.resolve(METALAKE, OBJECT).length);
    Assertions.assertEquals(0, resolver.resolveMatches(METALAKE, OBJECT).length);
  }

  @Test
  public void testSelectPolicyWhenAnySelectorMatches() throws Exception {
    TagEntity domain = tag(1L, "domain", TagAssignment.ofValues("finance"));
    TagEntity classified = tag(2L, "classified", TagAssignment.ofValues("public"));
    PolicyEntity policy = policy(10L, "policy", true);
    when(effectiveTagResolver.resolveWithSources(METALAKE, OBJECT))
        .thenReturn(
            new ResolvedTag[] {
              new ResolvedTag(domain, OBJECT), new ResolvedTag(classified, OBJECT)
            });
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

    Assertions.assertArrayEquals(new PolicyEntity[] {policy}, resolver.resolve(METALAKE, OBJECT));
  }

  @Test
  public void testChildTagValueOverrideDropsAncestorTagValuePolicy() throws Exception {
    // Effective tags after override: table data_domain=risk (catalog finance is shadowed).
    TagEntity domain =
        tag(1L, "data_domain", TagAssignment.ofValues("risk")).copyWithInherited(false);
    PolicyEntity financePolicy = policy(30L, "finance-policy", true);
    when(effectiveTagResolver.resolveWithSources(METALAKE, OBJECT))
        .thenReturn(new ResolvedTag[] {new ResolvedTag(domain, OBJECT)});
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(NameIdentifierUtil.ofTag(METALAKE, "data_domain")),
            Entity.EntityType.TAG))
        .thenReturn(
            Collections.singletonList(
                relation(domain, financePolicy, TagValueSelector.of("finance"))));

    Assertions.assertEquals(0, resolver.resolve(METALAKE, OBJECT).length);
    Assertions.assertEquals(0, resolver.resolveMatches(METALAKE, OBJECT).length);
  }

  @Test
  public void testBatchSharesPolicyReadAndPreservesSelectors() throws Exception {
    MetadataObject column =
        MetadataObjects.of(
            Arrays.asList("catalog", "schema", "table", "column"), MetadataObject.Type.COLUMN);
    TagEntity tableTag = tag(1L, "domain", TagAssignment.ofValues("finance"));
    TagEntity columnTag = tag(1L, "domain", TagAssignment.ofValues("engineering"));
    PolicyEntity policy = policy(10L, "selected", true);
    Map<MetadataObject, ResolvedTag[]> tags = new LinkedHashMap<>();
    tags.put(
        OBJECT, new ResolvedTag[] {new ResolvedTag(tableTag.copyWithInherited(false), OBJECT)});
    tags.put(
        column, new ResolvedTag[] {new ResolvedTag(columnTag.copyWithInherited(true), CATALOG)});
    when(effectiveTagResolver.resolveWithSources(METALAKE, List.of(OBJECT, column)))
        .thenReturn(tags);
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            List.of(NameIdentifierUtil.ofTag(METALAKE, "domain")),
            Entity.EntityType.TAG))
        .thenReturn(List.of(relation(tableTag, policy, TagValueSelector.of("finance"))));

    Map<MetadataObject, PolicyEntity[]> result =
        resolver.resolve(METALAKE, List.of(OBJECT, column));

    Assertions.assertEquals(1, result.get(OBJECT).length);
    Assertions.assertFalse(result.get(OBJECT)[0].inherited().orElseThrow());
    Assertions.assertEquals(0, result.get(column).length);
    verify(relationOperations, times(1))
        .batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            List.of(NameIdentifierUtil.ofTag(METALAKE, "domain")),
            Entity.EntityType.TAG);
  }

  /** Verifies distinct column tags do not cause every column to scan all policy relations. */
  @Test
  public void testBatchVisitsOnlyRelationsForEffectiveTags() throws Exception {
    List<MetadataObject> objects = new ArrayList<>();
    Map<MetadataObject, ResolvedTag[]> tags = new LinkedHashMap<>();
    List<NameIdentifier> tagIdentifiers = new ArrayList<>();
    List<RelationalEntity<?>> relations = new ArrayList<>();
    List<PolicyEntity> expectedPolicies = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      MetadataObject column =
          MetadataObjects.of(
              List.of("catalog", "schema", "table", "column_" + i), MetadataObject.Type.COLUMN);
      TagEntity tag = tag(i, "tag_" + i, TagAssignment.noValue()).copyWithInherited(false);
      PolicyEntity policy = policy(1000L + i, "policy_" + i, true);
      objects.add(column);
      tags.put(column, new ResolvedTag[] {new ResolvedTag(tag, column)});
      tagIdentifiers.add(tag.nameIdentifier());
      relations.add(spy(relation(tag, policy, AllValuesSelector.get())));
      expectedPolicies.add(policy);
    }
    MetadataObject untagged =
        MetadataObjects.of(
            List.of("catalog", "schema", "table", "untagged"), MetadataObject.Type.COLUMN);
    objects.add(untagged);
    tags.put(untagged, new ResolvedTag[0]);
    when(effectiveTagResolver.resolveWithSources(METALAKE, objects)).thenReturn(tags);
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL, tagIdentifiers, Entity.EntityType.TAG))
        .thenReturn(relations);

    Map<MetadataObject, PolicyEntity[]> result = resolver.resolve(METALAKE, objects);

    Assertions.assertEquals(objects.size(), result.size());
    for (int i = 0; i < expectedPolicies.size(); i++) {
      Assertions.assertArrayEquals(
          new PolicyEntity[] {expectedPolicies.get(i)}, result.get(objects.get(i)));
      // One visit to build the index and one visit for the column that owns this tag.
      verify(relations.get(i), times(2)).source();
    }
    Assertions.assertEquals(0, result.get(untagged).length);
    verify(relationOperations, times(1))
        .batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL, tagIdentifiers, Entity.EntityType.TAG);
  }

  /**
   * Verifies indexed matching retains relation order, deduplication and direct-match precedence.
   */
  @Test
  public void testBatchIndexPreservesPolicyOrderAndInheritance() throws Exception {
    TagEntity direct = tag(1L, "direct", TagAssignment.ofValues("finance"));
    TagEntity inherited = tag(2L, "inherited", TagAssignment.noValue()).copyWithInherited(true);
    TagEntity unrelated = tag(3L, "unrelated", TagAssignment.noValue());
    PolicyEntity first = policy(10L, "first", true);
    PolicyEntity second = policy(11L, "second", true);
    PolicyEntity disabled = policy(12L, "disabled", false);
    Map<MetadataObject, ResolvedTag[]> tags = new LinkedHashMap<>();
    tags.put(
        OBJECT,
        new ResolvedTag[] {new ResolvedTag(direct, OBJECT), new ResolvedTag(inherited, CATALOG)});
    tags.put(CATALOG, new ResolvedTag[] {new ResolvedTag(unrelated, CATALOG)});
    when(effectiveTagResolver.resolveWithSources(METALAKE, List.of(OBJECT, CATALOG)))
        .thenReturn(tags);
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            List.of(
                direct.nameIdentifier(), inherited.nameIdentifier(), unrelated.nameIdentifier()),
            Entity.EntityType.TAG))
        .thenReturn(
            List.of(
                relation(inherited, first, AllValuesSelector.get()),
                relation(unrelated, second, AllValuesSelector.get()),
                relation(direct, second, TagValueSelector.of("finance")),
                relation(direct, first, TagValueSelector.of("finance")),
                relation(direct, disabled, AllValuesSelector.get())));

    Map<MetadataObject, PolicyEntity[]> result =
        resolver.resolve(METALAKE, List.of(OBJECT, CATALOG));

    Assertions.assertArrayEquals(new PolicyEntity[] {first, second}, result.get(OBJECT));
    Assertions.assertFalse(result.get(OBJECT)[0].inherited().orElseThrow());
    Assertions.assertFalse(result.get(OBJECT)[1].inherited().orElseThrow());
    Assertions.assertArrayEquals(new PolicyEntity[] {second}, result.get(CATALOG));
  }

  @Test
  void testReadsDirectTagsAndPreservesBindingSelectors() throws Exception {
    stubTags(OBJECT, List.of(bronze));
    stubBindings(
        Arrays.asList(
            relation(PolicyAssociationSelectorSerde.serialize(TagValueSelector.of("gold"))),
            relation(null)));

    Assertions.assertArrayEquals(
        new String[] {"bronze"},
        resolver.loadTags(METALAKE, OBJECT).get(0).assignment().orElseThrow().values());
    List<ResolvedTagPolicy> bindings = resolver.loadPolicyBindings(METALAKE, List.of(bronze));
    Assertions.assertEquals(2, bindings.size());
    Assertions.assertEquals(TagValueSelector.of("gold"), bindings.get(0).selector());
    Assertions.assertSame(AllValuesSelector.get(), bindings.get(1).selector());
    Assertions.assertArrayEquals(
        new String[] {"bronze"}, bindings.get(0).tag().assignment().orElseThrow().values());
    Assertions.assertEquals(10L, bindings.get(0).policy().id());
  }

  @Test
  void testPointAndPreloadedReadersUseTheSameOverrideSemantics() throws Exception {
    stubTags(OBJECT, List.of(bronze));
    stubTags(SCHEMA, Collections.emptyList());
    stubTags(CATALOG, List.of(gold));
    stubBindings(
        List.of(relation(PolicyAssociationSelectorSerde.serialize(TagValueSelector.of("gold")))));
    ObjectPolicyResolver preloaded = mock(ObjectPolicyResolver.class);
    when(preloaded.loadTags(METALAKE, OBJECT)).thenReturn(List.of(bronze));
    when(preloaded.loadTags(METALAKE, CATALOG)).thenReturn(List.of(gold));
    when(preloaded.loadPolicyBindings(eq(METALAKE), anyList()))
        .thenAnswer(
            invocation -> {
              List<TagEntity> tags = invocation.getArgument(1);
              return List.of(
                  new ResolvedTagPolicy(tags.get(0), access, TagValueSelector.of("gold")));
            });

    for (ObjectPolicyResolver input : Arrays.asList(resolver, preloaded)) {
      List<ResolvedTagPolicy> result =
          new TagPolicyResolver(input).resolve(METALAKE, OBJECT, new AuthorizationRequestContext());
      Assertions.assertArrayEquals(
          new String[] {"bronze"}, result.get(0).tag().assignment().orElseThrow().values());
      Assertions.assertFalse(
          TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, Set.of("analyst"), result)
              .isPresent());
      Assertions.assertEquals(1L, result.get(0).tag().id());
      Assertions.assertEquals(10L, result.get(0).policy().id());
    }
  }

  @Test
  void testTagStorageFailurePropagates() throws Exception {
    IOException failure = new IOException("unavailable");
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, OBJECT),
            Entity.EntityType.TABLE))
        .thenThrow(failure);
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
                UncheckedIOException.class, () -> resolver.loadTags(METALAKE, OBJECT))
            .getCause());
  }

  @Test
  void testPolicyStorageFailurePropagates() throws Exception {
    IOException failure = new IOException("unavailable");
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            List.of(bronze.nameIdentifier()),
            Entity.EntityType.TAG))
        .thenThrow(failure);
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
                UncheckedIOException.class,
                () -> resolver.loadPolicyBindings(METALAKE, List.of(bronze)))
            .getCause());
  }

  @Test
  void testMalformedSelectorDoesNotBecomePresenceMatch() throws Exception {
    for (String invalid : Arrays.asList("{", "null", "{\"type\":\"UNKNOWN\"}")) {
      stubBindings(List.of(relation(null), relation(invalid)));
      IllegalStateException failure =
          Assertions.assertThrows(
              IllegalStateException.class,
              () -> resolver.loadPolicyBindings(METALAKE, List.of(bronze)));
      Assertions.assertInstanceOf(IllegalArgumentException.class, failure.getCause());
      Assertions.assertTrue(failure.getMessage().contains(METALAKE));
      Assertions.assertTrue(failure.getMessage().contains("tag ID 1"));
      Assertions.assertTrue(failure.getMessage().contains("policy ID 10"));
    }
  }

  @Test
  void testMissingObjectHasNoDirectTags() throws Exception {
    for (MetadataObject missing : Arrays.asList(OBJECT, SCHEMA, CATALOG)) {
      when(relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              MetadataObjectUtil.toEntityIdent(METALAKE, missing),
              MetadataObjectUtil.toEntityType(missing)))
          .thenThrow(new NoSuchEntityException("Missing object %s", missing));
      Assertions.assertTrue(resolver.loadTags(METALAKE, missing).isEmpty());
    }
    Assertions.assertTrue(
        new TagPolicyResolver(resolver)
            .resolve(METALAKE, OBJECT, new AuthorizationRequestContext())
            .isEmpty());
  }

  @Test
  void testMissingObjectDoesNotPreventInheritedPolicyResolution() throws Exception {
    stubTags(CATALOG, List.of(gold));
    stubBindings(List.of(relation(null)));
    for (MetadataObject missing : Arrays.asList(OBJECT, SCHEMA)) {
      stubTags(OBJECT, Collections.emptyList());
      stubTags(SCHEMA, Collections.emptyList());
      when(relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              MetadataObjectUtil.toEntityIdent(METALAKE, missing),
              MetadataObjectUtil.toEntityType(missing)))
          .thenThrow(new NoSuchEntityException("Missing object %s", missing));

      List<ResolvedTagPolicy> bindings =
          new TagPolicyResolver(resolver)
              .resolve(METALAKE, OBJECT, new AuthorizationRequestContext());
      Assertions.assertEquals(1, bindings.size());
      Assertions.assertTrue(
          TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, Set.of("analyst"), bindings)
              .isPresent());
      Assertions.assertTrue(
          TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, Set.of("unrelated"), bindings)
              .isEmpty());
    }
  }

  @Test
  void testDuplicateTagNamesAreRejectedBeforeReadingPolicies() {
    IllegalArgumentException failure =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> resolver.loadPolicyBindings(METALAKE, List.of(bronze, gold)));
    Assertions.assertTrue(failure.getMessage().contains("certified"));
    verifyNoInteractions(relationOperations);
  }

  @Test
  void testNoTagsDoNotTriggerPolicyReads() {
    Assertions.assertTrue(resolver.loadPolicyBindings(METALAKE, Collections.emptyList()).isEmpty());
    verifyNoInteractions(relationOperations);
  }

  private void stubTags(MetadataObject object, List<TagEntity> tags) throws IOException {
    doReturn(tags)
        .when(relationOperations)
        .listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, object),
            MetadataObjectUtil.toEntityType(object));
  }

  private void stubBindings(List<RelationalEntity<?>> bindings) throws IOException {
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            List.of(bronze.nameIdentifier()),
            Entity.EntityType.TAG))
        .thenReturn(bindings);
  }

  private RelationalEntity<PolicyEntity> relation(String selector) {
    return new RelationalEntity<>(
        SupportsRelationOperations.Type.POLICY_TAG_REL,
        gold.nameIdentifier(),
        Entity.EntityType.TAG,
        access,
        selector);
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
