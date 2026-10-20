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
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.relational.service.PolicyTagRelService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.service.TagMetaService;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestNearestPolicyTableEnumerator {

  private static final String METALAKE = "metalake";

  private PolicyTagRelService policyTagRelService;
  private TagMetaService tagMetaService;
  private SchemaMetaService schemaMetaService;
  private TableMetaService tableMetaService;
  private EntityStore entityStore;
  private SupportsRelationOperations relationOperations;
  private NearestPolicyTableEnumerator enumerator;

  @BeforeEach
  void setUp() {
    policyTagRelService = mock(PolicyTagRelService.class);
    tagMetaService = mock(TagMetaService.class);
    schemaMetaService = mock(SchemaMetaService.class);
    tableMetaService = mock(TableMetaService.class);
    entityStore = mock(EntityStore.class);
    relationOperations = mock(SupportsRelationOperations.class);
    when(entityStore.relationOperations()).thenReturn(relationOperations);
    enumerator =
        new NearestPolicyTableEnumerator(
            policyTagRelService,
            tagMetaService,
            schemaMetaService,
            tableMetaService,
            () -> entityStore);
  }

  @Test
  void testDirectTableIncludedWhenNearest() throws Exception {
    PolicyEntity policy = policy(10L, "compaction");
    TagEntity tag = tag(1L, "maint");
    when(policyTagRelService.listRelations(
            eq(List.of(NameIdentifierUtil.ofPolicy(METALAKE, "compaction"))),
            eq(Entity.EntityType.POLICY)))
        .thenReturn(List.of(policyTagEdge(policy, tag)));
    when(tagMetaService.listAssociatedMetadataObjectsForTag(tag.nameIdentifier()))
        .thenReturn(
            List.of(
                GenericEntity.builder()
                    .withId(100L)
                    .withName("catalog.schema.t1")
                    .withEntityType(Entity.EntityType.TABLE)
                    .build()));

    // Winning path: tags on table select this policy
    MetadataObject table =
        MetadataObjects.of(
            java.util.Arrays.asList("catalog", "schema", "t1"), MetadataObject.Type.TABLE);
    doReturn(List.of(tag))
        .when(relationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
            eq(MetadataObjectUtil.toEntityIdent(METALAKE, table)),
            eq(Entity.EntityType.TABLE));
    when(relationOperations.batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.POLICY_TAG_REL),
            eq(List.of(NameIdentifierUtil.ofTag(METALAKE, "maint"))),
            eq(Entity.EntityType.TAG)))
        .thenReturn(List.of(policyFromTag(tag, policy)));
    stubEmptyParents(table);

    List<ExpandTableCandidate> got = enumerator.listTableIds(policy, 10, allowAll(), 0L);
    Assertions.assertEquals(1, got.size());
    Assertions.assertEquals(100L, got.get(0).tableId());
    Assertions.assertEquals(MetadataObject.Type.TABLE, got.get(0).winnerLevel());
  }

  @Test
  void testSchemaExpansionSkipsOverriddenTable() throws Exception {
    PolicyEntity catalogPolicy = policy(10L, "catalog-compaction");
    PolicyEntity tablePolicy = policy(11L, "table-compaction");
    TagEntity catalogTag = tag(1L, "maint");
    TagEntity tableTag = tag(2L, "maint-table");

    when(policyTagRelService.listRelations(
            eq(List.of(NameIdentifierUtil.ofPolicy(METALAKE, "catalog-compaction"))),
            eq(Entity.EntityType.POLICY)))
        .thenReturn(List.of(policyTagEdge(catalogPolicy, catalogTag)));
    when(tagMetaService.listAssociatedMetadataObjectsForTag(catalogTag.nameIdentifier()))
        .thenReturn(
            List.of(
                GenericEntity.builder()
                    .withId(1L)
                    .withName("catalog.schema")
                    .withEntityType(Entity.EntityType.SCHEMA)
                    .build()));

    TableEntity t1 =
        TableEntity.builder()
            .withId(100L)
            .withName("t1")
            .withNamespace(NamespaceUtil.ofTable(METALAKE, "catalog", "schema"))
            .withAuditInfo(audit())
            .build();
    TableEntity t2 =
        TableEntity.builder()
            .withId(101L)
            .withName("t2")
            .withNamespace(NamespaceUtil.ofTable(METALAKE, "catalog", "schema"))
            .withAuditInfo(audit())
            .build();
    when(tableMetaService.listTablesByNamespace(
            eq(NamespaceUtil.ofTable(METALAKE, "catalog", "schema"))))
        .thenReturn(List.of(t1, t2));

    MetadataObject table1 =
        MetadataObjects.of(
            java.util.Arrays.asList("catalog", "schema", "t1"), MetadataObject.Type.TABLE);
    MetadataObject table2 =
        MetadataObjects.of(
            java.util.Arrays.asList("catalog", "schema", "t2"), MetadataObject.Type.TABLE);
    MetadataObject schema =
        MetadataObjects.of(
            java.util.Arrays.asList("catalog", "schema"), MetadataObject.Type.SCHEMA);
    MetadataObject catalog =
        MetadataObjects.of(
            java.util.Collections.singletonList("catalog"), MetadataObject.Type.CATALOG);

    // t1 overridden by table-local policy 11
    doReturn(List.of(tableTag))
        .when(relationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
            eq(MetadataObjectUtil.toEntityIdent(METALAKE, table1)),
            eq(Entity.EntityType.TABLE));
    when(relationOperations.batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.POLICY_TAG_REL),
            eq(List.of(NameIdentifierUtil.ofTag(METALAKE, "maint-table"))),
            eq(Entity.EntityType.TAG)))
        .thenReturn(List.of(policyFromTag(tableTag, tablePolicy)));

    // t2 inherits catalog policy via schema-level tag
    doReturn(List.of())
        .when(relationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
            eq(MetadataObjectUtil.toEntityIdent(METALAKE, table2)),
            eq(Entity.EntityType.TABLE));
    doReturn(List.of(catalogTag))
        .when(relationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
            eq(MetadataObjectUtil.toEntityIdent(METALAKE, schema)),
            eq(Entity.EntityType.SCHEMA));
    when(relationOperations.batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.POLICY_TAG_REL),
            eq(List.of(NameIdentifierUtil.ofTag(METALAKE, "maint"))),
            eq(Entity.EntityType.TAG)))
        .thenReturn(List.of(policyFromTag(catalogTag, catalogPolicy)));
    doReturn(List.of())
        .when(relationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
            eq(MetadataObjectUtil.toEntityIdent(METALAKE, catalog)),
            eq(Entity.EntityType.CATALOG));

    List<ExpandTableCandidate> got = enumerator.listTableIds(catalogPolicy, 10, allowAll(), 0L);
    Assertions.assertEquals(1, got.size());
    Assertions.assertEquals(101L, got.get(0).tableId());
    Assertions.assertEquals(MetadataObject.Type.SCHEMA, got.get(0).winnerLevel());
  }

  @Test
  void testFillsBatchSkippingCooldownWinners() throws Exception {
    PolicyEntity policy = policy(10L, "compaction");
    TagEntity tag = tag(1L, "maint");
    when(policyTagRelService.listRelations(
            eq(List.of(NameIdentifierUtil.ofPolicy(METALAKE, "compaction"))),
            eq(Entity.EntityType.POLICY)))
        .thenReturn(List.of(policyTagEdge(policy, tag)));
    when(tagMetaService.listAssociatedMetadataObjectsForTag(tag.nameIdentifier()))
        .thenReturn(
            List.of(
                GenericEntity.builder()
                    .withId(1L)
                    .withName("catalog.schema")
                    .withEntityType(Entity.EntityType.SCHEMA)
                    .build()));

    TableEntity t1 =
        TableEntity.builder()
            .withId(100L)
            .withName("t1")
            .withNamespace(NamespaceUtil.ofTable(METALAKE, "catalog", "schema"))
            .withAuditInfo(audit())
            .build();
    TableEntity t2 =
        TableEntity.builder()
            .withId(101L)
            .withName("t2")
            .withNamespace(NamespaceUtil.ofTable(METALAKE, "catalog", "schema"))
            .withAuditInfo(audit())
            .build();
    TableEntity t3 =
        TableEntity.builder()
            .withId(102L)
            .withName("t3")
            .withNamespace(NamespaceUtil.ofTable(METALAKE, "catalog", "schema"))
            .withAuditInfo(audit())
            .build();
    when(tableMetaService.listTablesByNamespace(
            eq(NamespaceUtil.ofTable(METALAKE, "catalog", "schema"))))
        .thenReturn(List.of(t1, t2, t3));

    for (String name : List.of("t1", "t2", "t3")) {
      MetadataObject table =
          MetadataObjects.of(
              java.util.Arrays.asList("catalog", "schema", name), MetadataObject.Type.TABLE);
      doReturn(List.of(tag))
          .when(relationOperations)
          .listEntitiesByRelation(
              eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              eq(MetadataObjectUtil.toEntityIdent(METALAKE, table)),
              eq(Entity.EntityType.TABLE));
      stubEmptyParents(table);
    }
    when(relationOperations.batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.POLICY_TAG_REL),
            eq(List.of(NameIdentifierUtil.ofTag(METALAKE, "maint"))),
            eq(Entity.EntityType.TAG)))
        .thenReturn(List.of(policyFromTag(tag, policy)));

    // First two winners are cooling down; limit=1 must keep scanning to t3.
    ExpandOccupancyGate gate = (tableId, policyId, minIntervalMs, nowMs) -> tableId != 102L;
    List<ExpandTableCandidate> got = enumerator.listTableIds(policy, 1, gate, 60_000L);
    Assertions.assertEquals(1, got.size());
    Assertions.assertEquals(102L, got.get(0).tableId());
  }

  @Test
  void testSameLevelConflictSkipsTable() throws Exception {
    PolicyEntity policy = policy(10L, "compaction");
    PolicyEntity other = policy(11L, "compaction-other");
    TagEntity tagA = tag(1L, "t-a");
    TagEntity tagB = tag(2L, "t-b");

    when(policyTagRelService.listRelations(
            eq(List.of(NameIdentifierUtil.ofPolicy(METALAKE, "compaction"))),
            eq(Entity.EntityType.POLICY)))
        .thenReturn(List.of(policyTagEdge(policy, tagA)));
    when(tagMetaService.listAssociatedMetadataObjectsForTag(tagA.nameIdentifier()))
        .thenReturn(
            List.of(
                GenericEntity.builder()
                    .withId(100L)
                    .withName("catalog.schema.t1")
                    .withEntityType(Entity.EntityType.TABLE)
                    .build()));

    MetadataObject table =
        MetadataObjects.of(
            java.util.Arrays.asList("catalog", "schema", "t1"), MetadataObject.Type.TABLE);
    doReturn(List.of(tagA, tagB))
        .when(relationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
            eq(MetadataObjectUtil.toEntityIdent(METALAKE, table)),
            eq(Entity.EntityType.TABLE));
    when(relationOperations.batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.POLICY_TAG_REL),
            eq(
                List.of(
                    NameIdentifierUtil.ofTag(METALAKE, "t-a"),
                    NameIdentifierUtil.ofTag(METALAKE, "t-b"))),
            eq(Entity.EntityType.TAG)))
        .thenReturn(List.of(policyFromTag(tagA, policy), policyFromTag(tagB, other)));

    Assertions.assertTrue(enumerator.listTableIds(policy, 10, allowAll(), 0L).isEmpty());
  }

  private static ExpandOccupancyGate allowAll() {
    return (tableId, policyId, minIntervalMs, nowMs) -> false;
  }

  private void stubEmptyParents(MetadataObject table) throws Exception {
    for (MetadataObject parent : MetadataObjectUtil.getParentMetadataObjects(table)) {
      doReturn(List.of())
          .when(relationOperations)
          .listEntitiesByRelation(
              eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              eq(MetadataObjectUtil.toEntityIdent(METALAKE, parent)),
              eq(MetadataObjectUtil.toEntityType(parent)));
    }
  }

  private static RelationalEntity<TagEntity> policyTagEdge(PolicyEntity policy, TagEntity tag) {
    return new RelationalEntity<>(
        SupportsRelationOperations.Type.POLICY_TAG_REL,
        policy.nameIdentifier(),
        Entity.EntityType.POLICY,
        tag,
        null);
  }

  private static RelationalEntity<PolicyEntity> policyFromTag(TagEntity tag, PolicyEntity policy) {
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
        .withAuditInfo(audit())
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
        .withAuditInfo(audit())
        .build();
  }

  private static AuditInfo audit() {
    return AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
  }
}
