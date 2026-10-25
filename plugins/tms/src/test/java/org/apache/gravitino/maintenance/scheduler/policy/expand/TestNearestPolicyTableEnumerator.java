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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.maintenance.scheduler.policy.expand.ExpandOccupancyGate;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
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
  private NearestMaintenancePolicyResolver nearest;
  private NearestPolicyTableEnumerator enumerator;

  @BeforeEach
  void setUp() {
    policyTagRelService = mock(PolicyTagRelService.class);
    tagMetaService = mock(TagMetaService.class);
    schemaMetaService = mock(SchemaMetaService.class);
    tableMetaService = mock(TableMetaService.class);
    nearest = mock(NearestMaintenancePolicyResolver.class);
    enumerator =
        new NearestPolicyTableEnumerator(
            policyTagRelService,
            tagMetaService,
            schemaMetaService,
            tableMetaService,
            () -> nearest);
    when(schemaMetaService.listSchemasByNamespace(any())).thenReturn(List.of());
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

    MetadataObject table =
        MetadataObjects.of(
            java.util.Arrays.asList("catalog", "schema", "t1"), MetadataObject.Type.TABLE);
    when(nearest.resolve(METALAKE, table, Policy.BuiltInType.ICEBERG_COMPACTION))
        .thenReturn(NearestMaintenancePolicyResolver.Decision.winner(10L, table));

    Assertions.assertEquals(
        List.of(100L), tableIds(enumerator.listTableIds(policy, 10, allowAll(), 0L)));
  }

  @Test
  void testSchemaExpansionSkipsOverriddenTable() throws Exception {
    PolicyEntity catalogPolicy = policy(10L, "catalog-compaction");
    TagEntity catalogTag = tag(1L, "maint");

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

    when(nearest.resolve(METALAKE, table1, Policy.BuiltInType.ICEBERG_COMPACTION))
        .thenReturn(NearestMaintenancePolicyResolver.Decision.winner(11L, table1));
    when(nearest.resolve(METALAKE, table2, Policy.BuiltInType.ICEBERG_COMPACTION))
        .thenReturn(NearestMaintenancePolicyResolver.Decision.winner(10L, schema));

    Assertions.assertEquals(
        List.of(101L), tableIds(enumerator.listTableIds(catalogPolicy, 10, allowAll(), 0L)));
  }

  @Test
  void testSchemaExpansionIncludesHierarchicalDescendantTables() throws Exception {
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
                    .withName("catalog.a")
                    .withEntityType(Entity.EntityType.SCHEMA)
                    .build()));

    when(schemaMetaService.listSchemasByNamespace(eq(NamespaceUtil.ofSchema(METALAKE, "catalog"))))
        .thenReturn(
            List.of(schema(2L, "a"), schema(3L, "a:b"), schema(4L, "ab"), schema(5L, "other")));
    when(tableMetaService.listTablesByNamespace(
            eq(NamespaceUtil.ofTable(METALAKE, "catalog", "a"))))
        .thenReturn(List.of());
    TableEntity childTable =
        TableEntity.builder()
            .withId(200L)
            .withName("t1")
            .withNamespace(NamespaceUtil.ofTable(METALAKE, "catalog", "a:b"))
            .withAuditInfo(audit())
            .build();
    when(tableMetaService.listTablesByNamespace(
            eq(NamespaceUtil.ofTable(METALAKE, "catalog", "a:b"))))
        .thenReturn(List.of(childTable));

    MetadataObject table =
        MetadataObjects.of(
            java.util.Arrays.asList("catalog", "a:b", "t1"), MetadataObject.Type.TABLE);
    MetadataObject parentSchema =
        MetadataObjects.of(java.util.Arrays.asList("catalog", "a"), MetadataObject.Type.SCHEMA);
    when(nearest.resolve(METALAKE, table, Policy.BuiltInType.ICEBERG_COMPACTION))
        .thenReturn(NearestMaintenancePolicyResolver.Decision.winner(10L, parentSchema));

    Assertions.assertEquals(
        List.of(200L), tableIds(enumerator.listTableIds(policy, 10, allowAll(), 0L)));
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

    when(nearest.resolve(
            eq(METALAKE), any(MetadataObject.class), eq(Policy.BuiltInType.ICEBERG_COMPACTION)))
        .thenAnswer(
            invocation -> {
              MetadataObject table = invocation.getArgument(1);
              return NearestMaintenancePolicyResolver.Decision.winner(10L, table);
            });

    ExpandOccupancyGate gate = (tableId, policyId, minIntervalMs, nowMs) -> tableId != 102L;
    Assertions.assertEquals(
        List.of(102L), tableIds(enumerator.listTableIds(policy, 1, gate, 60_000L)));
  }

  @Test
  void testSameLevelConflictSkipsTable() throws Exception {
    PolicyEntity policy = policy(10L, "compaction");
    TagEntity tagA = tag(1L, "t-a");

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
    when(nearest.resolve(METALAKE, table, Policy.BuiltInType.ICEBERG_COMPACTION))
        .thenReturn(NearestMaintenancePolicyResolver.Decision.conflict(table, List.of(10L, 11L)));

    Assertions.assertEquals(
        List.of(), tableIds(enumerator.listTableIds(policy, 10, allowAll(), 0L)));
  }

  private static List<Long> tableIds(List<ExpandTableCandidate> candidates) {
    return candidates.stream().map(ExpandTableCandidate::tableId).toList();
  }

  private static ExpandOccupancyGate allowAll() {
    return (tableId, policyId, minIntervalMs, nowMs) -> false;
  }

  private static RelationalEntity<TagEntity> policyTagEdge(PolicyEntity policy, TagEntity tag) {
    return new RelationalEntity<>(
        SupportsRelationOperations.Type.POLICY_TAG_REL,
        policy.nameIdentifier(),
        Entity.EntityType.POLICY,
        tag,
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

  private static SchemaEntity schema(long id, String name) {
    return SchemaEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofSchema(METALAKE, "catalog"))
        .withAuditInfo(audit())
        .build();
  }

  private static AuditInfo audit() {
    return AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
  }
}
