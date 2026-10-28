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

package org.apache.gravitino.authorization;

import static org.apache.gravitino.authorization.TagPolicyTestUtils.METALAKE;
import static org.apache.gravitino.authorization.TagPolicyTestUtils.policy;
import static org.apache.gravitino.authorization.TagPolicyTestUtils.tag;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.TagPolicyTestUtils.InMemoryReader;
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.TagValueSelector;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests assignment and policy-binding reads through the entity-store relation API. */
class TestEntityStoreTagPolicyReader {
  private static final MetadataObject TABLE =
      MetadataObjects.of("catalog.finance", "orders", MetadataObject.Type.TABLE);
  private static final MetadataObject SCHEMA =
      MetadataObjects.of("catalog", "finance", MetadataObject.Type.SCHEMA);
  private static final MetadataObject CATALOG =
      MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG);

  private SupportsRelationOperations relations;
  private EntityStoreTagPolicyReader reader;
  private TagEntity gold;
  private TagEntity bronze;
  private PolicyEntity access;

  @BeforeEach
  void setUp() {
    EntityStore store = mock(EntityStore.class);
    relations = mock(SupportsRelationOperations.class);
    when(store.relationOperations()).thenReturn(relations);
    reader = new EntityStoreTagPolicyReader(store);
    gold = tag(1L, "certified", TagAssignment.ofValues("gold"));
    bronze = tag(1L, "certified", TagAssignment.ofValues("bronze"));
    access = policy(10L, true, List.of(Privilege.Name.SELECT_TABLE), List.of("analyst"));
  }

  @Test
  void testReadsDirectTagsAndPreservesBindingSelectors() throws Exception {
    stubTags(TABLE, List.of(bronze));
    stubBindings(
        Arrays.asList(
            relation(PolicyAssociationSelectorSerde.serialize(TagValueSelector.of("gold"))),
            relation(null)));

    Assertions.assertArrayEquals(
        new String[] {"bronze"},
        reader.loadTags(METALAKE, TABLE).get(0).assignment().orElseThrow().values());
    List<ResolvedTagPolicy> bindings = reader.loadPolicyBindings(METALAKE, List.of(bronze));
    Assertions.assertEquals(2, bindings.size());
    Assertions.assertEquals(TagValueSelector.of("gold"), bindings.get(0).selector());
    Assertions.assertSame(AllValuesSelector.get(), bindings.get(1).selector());
    Assertions.assertArrayEquals(
        new String[] {"bronze"}, bindings.get(0).tag().assignment().orElseThrow().values());
    Assertions.assertEquals(10L, bindings.get(0).policy().id());
  }

  @Test
  void testPointAndPreloadedReadersUseTheSameOverrideSemantics() throws Exception {
    stubTags(TABLE, List.of(bronze));
    stubTags(SCHEMA, Collections.emptyList());
    stubTags(CATALOG, List.of(gold));
    stubBindings(
        List.of(relation(PolicyAssociationSelectorSerde.serialize(TagValueSelector.of("gold")))));
    InMemoryReader preloaded = new InMemoryReader();
    preloaded.assignments.put(TABLE, List.of(bronze));
    preloaded.assignments.put(CATALOG, List.of(gold));
    preloaded.bind(gold, access, TagValueSelector.of("gold"));

    for (TagPolicyReader input : Arrays.asList(reader, preloaded)) {
      List<ResolvedTagPolicy> result =
          new TagPolicyResolver(input).resolve(METALAKE, TABLE, new AuthorizationRequestContext());
      Assertions.assertArrayEquals(
          new String[] {"bronze"}, result.get(0).tag().assignment().orElseThrow().values());
      Assertions.assertEquals(1L, result.get(0).tag().id());
      Assertions.assertEquals(10L, result.get(0).policy().id());
    }
  }

  @Test
  void testTagStorageFailurePropagates() throws Exception {
    IOException failure = new IOException("unavailable");
    when(relations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, TABLE),
            Entity.EntityType.TABLE))
        .thenThrow(failure);
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(UncheckedIOException.class, () -> reader.loadTags(METALAKE, TABLE))
            .getCause());
  }

  @Test
  void testPolicyStorageFailurePropagates() throws Exception {
    IOException failure = new IOException("unavailable");
    when(relations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            List.of(bronze.nameIdentifier()),
            Entity.EntityType.TAG))
        .thenThrow(failure);
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
                UncheckedIOException.class,
                () -> reader.loadPolicyBindings(METALAKE, List.of(bronze)))
            .getCause());
  }

  @Test
  void testMalformedSelectorDoesNotBecomePresenceMatch() throws Exception {
    for (String invalid : Arrays.asList("{", "null", "{\"type\":\"UNKNOWN\"}")) {
      stubBindings(List.of(relation(invalid)));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> reader.loadPolicyBindings(METALAKE, List.of(bronze)));
    }
  }

  @Test
  void testNoTagsDoNotTriggerPolicyReads() {
    Assertions.assertTrue(reader.loadPolicyBindings(METALAKE, Collections.emptyList()).isEmpty());
    verifyNoInteractions(relations);
  }

  private void stubTags(MetadataObject object, List<TagEntity> tags) throws IOException {
    when(relations.<TagEntity>listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, object),
            MetadataObjectUtil.toEntityType(object)))
        .thenReturn(tags);
  }

  private void stubBindings(List<RelationalEntity<?>> bindings) throws IOException {
    when(relations.batchListEntitiesByRelation(
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
}
