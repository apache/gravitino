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
package org.apache.gravitino.maintenance.scheduler;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestNearestMaintenancePolicyResolver {

  private static final String METALAKE = "metalake";
  private static final MetadataObject TABLE =
      MetadataObjects.of(
          java.util.Arrays.asList("catalog", "schema", "table"), MetadataObject.Type.TABLE);
  private static final MetadataObject SCHEMA =
      MetadataObjects.of(java.util.Arrays.asList("catalog", "schema"), MetadataObject.Type.SCHEMA);
  private static final MetadataObject CATALOG =
      MetadataObjects.of(
          java.util.Collections.singletonList("catalog"), MetadataObject.Type.CATALOG);

  private EntityStore entityStore;
  private SupportsRelationOperations relationOperations;
  private NearestMaintenancePolicyResolver resolver;

  @BeforeEach
  void setUp() {
    entityStore = mock(EntityStore.class);
    relationOperations = mock(SupportsRelationOperations.class);
    when(entityStore.relationOperations()).thenReturn(relationOperations);
    resolver = new NearestMaintenancePolicyResolver(entityStore);
  }

  @Test
  void testTableLevelBeatsCatalogLevel() throws Exception {
    PolicyEntity tablePolicy = policy(10L, "table-compaction");
    PolicyEntity catalogPolicy = policy(20L, "catalog-compaction");
    TagEntity tableTag = tag(1L, "t-table");
    TagEntity catalogTag = tag(2L, "t-catalog");

    stubDirectTags(TABLE, List.of(tableTag));
    stubDirectTags(SCHEMA, List.of());
    stubDirectTags(CATALOG, List.of(catalogTag));

    when(relationOperations.batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.POLICY_TAG_REL),
            eq(List.of(NameIdentifierUtil.ofTag(METALAKE, "t-table"))),
            eq(Entity.EntityType.TAG)))
        .thenReturn(List.of(relation(tableTag, tablePolicy)));
    when(relationOperations.batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.POLICY_TAG_REL),
            eq(List.of(NameIdentifierUtil.ofTag(METALAKE, "t-catalog"))),
            eq(Entity.EntityType.TAG)))
        .thenReturn(List.of(relation(catalogTag, catalogPolicy)));

    NearestPolicyDecision decision =
        resolver.resolve(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION);
    Assertions.assertEquals(NearestPolicyDecision.Kind.WINNER, decision.kind());
    Assertions.assertEquals(java.util.Optional.of(10L), decision.winnerPolicyId());
    Assertions.assertEquals(TABLE, decision.winnerLevel());
  }

  @Test
  void testFallsBackToAncestorWhenTableHasNoMatch() throws Exception {
    PolicyEntity catalogPolicy = policy(20L, "catalog-compaction");
    TagEntity catalogTag = tag(2L, "t-catalog");

    stubDirectTags(TABLE, List.of());
    stubDirectTags(SCHEMA, List.of());
    stubDirectTags(CATALOG, List.of(catalogTag));

    when(relationOperations.batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.POLICY_TAG_REL),
            eq(List.of(NameIdentifierUtil.ofTag(METALAKE, "t-catalog"))),
            eq(Entity.EntityType.TAG)))
        .thenReturn(List.of(relation(catalogTag, catalogPolicy)));

    NearestPolicyDecision decision =
        resolver.resolve(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION);
    Assertions.assertEquals(NearestPolicyDecision.Kind.WINNER, decision.kind());
    Assertions.assertEquals(java.util.Optional.of(20L), decision.winnerPolicyId());
    Assertions.assertEquals(CATALOG, decision.winnerLevel());
  }

  @Test
  void testEmptyWhenNoMatchingType() throws Exception {
    stubDirectTags(TABLE, List.of());
    stubDirectTags(SCHEMA, List.of());
    stubDirectTags(CATALOG, List.of());

    NearestPolicyDecision decision =
        resolver.resolve(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION);
    Assertions.assertEquals(NearestPolicyDecision.Kind.NONE, decision.kind());
  }

  @Test
  void testSameLevelConflict() throws Exception {
    PolicyEntity p1 = policy(10L, "compaction-a");
    PolicyEntity p2 = policy(11L, "compaction-b");
    TagEntity tagA = tag(1L, "t-a");
    TagEntity tagB = tag(2L, "t-b");

    stubDirectTags(TABLE, List.of(tagA, tagB));

    when(relationOperations.batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.POLICY_TAG_REL),
            eq(
                List.of(
                    NameIdentifierUtil.ofTag(METALAKE, "t-a"),
                    NameIdentifierUtil.ofTag(METALAKE, "t-b"))),
            eq(Entity.EntityType.TAG)))
        .thenReturn(List.of(relation(tagA, p1), relation(tagB, p2)));

    NearestPolicyDecision decision =
        resolver.resolve(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION);
    Assertions.assertEquals(NearestPolicyDecision.Kind.CONFLICT, decision.kind());
    Assertions.assertEquals(TABLE, decision.conflictLevel());
    Assertions.assertEquals(List.of(10L, 11L), decision.conflictingPolicyIds());
    Assertions.assertTrue(decision.winnerPolicyId().isEmpty());
  }

  private void stubDirectTags(MetadataObject object, List<TagEntity> tags) throws Exception {
    doReturn(tags)
        .when(relationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
            eq(MetadataObjectUtil.toEntityIdent(METALAKE, object)),
            eq(MetadataObjectUtil.toEntityType(object)));
  }

  private static RelationalEntity<PolicyEntity> relation(TagEntity tag, PolicyEntity policy) {
    return new RelationalEntity<>(
        SupportsRelationOperations.Type.POLICY_TAG_REL,
        tag.nameIdentifier(),
        Entity.EntityType.TAG,
        policy,
        null);
  }

  private static TagEntity tag(long id, String name) {
    return TagEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofTag(METALAKE))
        .withAssignment(TagAssignment.noValue())
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }

  private static PolicyEntity policy(long id, String name) {
    return PolicyEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
        .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
        .withEnabled(true)
        .withContent(
            PolicyContents.icebergDataCompaction(
                IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
                IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
                IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT,
                IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
                IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM,
                IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
                IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
                null,
                "0 0 2 * * ?",
                null,
                null,
                null,
                null))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
