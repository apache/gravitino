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
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/** JDBC coverage for {@link PolicyMetaService#resolveNearestPolicyIds}. */
public class TestNearestPolicyResolution extends TestJDBCBackend {

  private static final String METALAKE = "nearest_policy_metalake";

  @TestTemplate
  public void testTableLevelBeatsCatalogAndEmptyWhenNone() throws IOException {
    createAndInsertMakeLake(METALAKE);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE, "cat");
    createAndInsertSchema(METALAKE, "cat", "schema");
    TableEntity table =
        createAndInsertTableEntity(NamespaceUtil.ofTable(METALAKE, "cat", "schema"), "t1");

    PolicyEntity tablePolicy = insertCompaction("table_compact");
    PolicyEntity catalogPolicy = insertCompaction("catalog_compact");
    TagEntity tableTag = insertTag("table_tag");
    TagEntity catalogTag = insertTag("catalog_tag");
    linkPolicy(tableTag, tablePolicy, null);
    linkPolicy(catalogTag, catalogPolicy, null);
    assignTag(table.nameIdentifier(), Entity.EntityType.TABLE, TagValue.noValue("table_tag"));
    assignTag(catalog.nameIdentifier(), Entity.EntityType.CATALOG, TagValue.noValue("catalog_tag"));

    PolicyMetaService service = PolicyMetaService.getInstance();
    Assertions.assertEquals(
        tablePolicy.id(),
        service
            .resolveNearestPolicyIds(List.of(table.id()), Policy.BuiltInType.ICEBERG_COMPACTION)
            .get(table.id()));

    TableEntity other =
        createAndInsertTableEntity(NamespaceUtil.ofTable(METALAKE, "cat", "schema"), "t2");
    Assertions.assertEquals(
        catalogPolicy.id(),
        service
            .resolveNearestPolicyIds(List.of(other.id()), Policy.BuiltInType.ICEBERG_COMPACTION)
            .get(other.id()));

    Assertions.assertNull(
        service
            .resolveNearestPolicyIds(
                List.of(table.id()), Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL)
            .get(table.id()));
  }

  @TestTemplate
  public void testHierarchicalAncestorSchemaAndNearerSchemaWins() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, "cat");
    SchemaEntity outer = createAndInsertSchema(METALAKE, "cat", "a");
    SchemaEntity middle = createAndInsertSchema(METALAKE, "cat", "a:b");
    createAndInsertSchema(METALAKE, "cat", "a:b:c");
    TableEntity table =
        createAndInsertTableEntity(NamespaceUtil.ofTable(METALAKE, "cat", "a:b:c"), "t1");

    PolicyEntity outerPolicy = insertCompaction("outer_compact");
    TagEntity outerTag = insertTag("outer_tag");
    linkPolicy(outerTag, outerPolicy, null);
    assignTag(outer.nameIdentifier(), Entity.EntityType.SCHEMA, TagValue.noValue("outer_tag"));

    PolicyMetaService service = PolicyMetaService.getInstance();
    Assertions.assertEquals(
        outerPolicy.id(),
        service
            .resolveNearestPolicyIds(List.of(table.id()), Policy.BuiltInType.ICEBERG_COMPACTION)
            .get(table.id()));

    PolicyEntity middlePolicy = insertCompaction("middle_compact");
    TagEntity middleTag = insertTag("middle_tag");
    linkPolicy(middleTag, middlePolicy, null);
    assignTag(middle.nameIdentifier(), Entity.EntityType.SCHEMA, TagValue.noValue("middle_tag"));
    Assertions.assertEquals(
        middlePolicy.id(),
        service
            .resolveNearestPolicyIds(List.of(table.id()), Policy.BuiltInType.ICEBERG_COMPACTION)
            .get(table.id()));
  }

  @TestTemplate
  public void testNamePrefixWithoutSeparatorIsNotAncestor() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, "cat");
    SchemaEntity a = createAndInsertSchema(METALAKE, "cat", "a");
    createAndInsertSchema(METALAKE, "cat", "ab");
    createAndInsertSchema(METALAKE, "cat", "ab:c");
    TableEntity table =
        createAndInsertTableEntity(NamespaceUtil.ofTable(METALAKE, "cat", "ab:c"), "t1");

    PolicyEntity policy = insertCompaction("a_compact");
    TagEntity tag = insertTag("a_tag");
    linkPolicy(tag, policy, null);
    assignTag(a.nameIdentifier(), Entity.EntityType.SCHEMA, TagValue.noValue("a_tag"));

    Assertions.assertNull(
        PolicyMetaService.getInstance()
            .resolveNearestPolicyIds(List.of(table.id()), Policy.BuiltInType.ICEBERG_COMPACTION)
            .get(table.id()));
  }

  @TestTemplate
  public void testBatchResolvesEachTable() throws IOException {
    createAndInsertMakeLake(METALAKE);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE, "cat");
    createAndInsertSchema(METALAKE, "cat", "s1");
    createAndInsertSchema(METALAKE, "cat", "s2");
    TableEntity tagged =
        createAndInsertTableEntity(NamespaceUtil.ofTable(METALAKE, "cat", "s1"), "t1");
    TableEntity inherited =
        createAndInsertTableEntity(NamespaceUtil.ofTable(METALAKE, "cat", "s2"), "t2");

    PolicyEntity tablePolicy = insertCompaction("table_compact");
    PolicyEntity catalogPolicy = insertCompaction("catalog_compact");
    TagEntity tableTag = insertTag("table_tag");
    TagEntity catalogTag = insertTag("catalog_tag");
    linkPolicy(tableTag, tablePolicy, null);
    linkPolicy(catalogTag, catalogPolicy, null);
    assignTag(tagged.nameIdentifier(), Entity.EntityType.TABLE, TagValue.noValue("table_tag"));
    assignTag(catalog.nameIdentifier(), Entity.EntityType.CATALOG, TagValue.noValue("catalog_tag"));

    Assertions.assertEquals(
        Map.of(tagged.id(), tablePolicy.id(), inherited.id(), catalogPolicy.id()),
        PolicyMetaService.getInstance()
            .resolveNearestPolicyIds(
                List.of(tagged.id(), inherited.id()), Policy.BuiltInType.ICEBERG_COMPACTION));
  }

  @TestTemplate
  public void testNearerTagValueOverridesAncestorValue() throws IOException {
    createAndInsertMakeLake(METALAKE);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE, "cat");
    createAndInsertSchema(METALAKE, "cat", "schema");
    TableEntity table =
        createAndInsertTableEntity(NamespaceUtil.ofTable(METALAKE, "cat", "schema"), "t1");

    PolicyEntity finance = insertCompaction("finance_compact");
    TagEntity domain = insertTag("domain");
    linkPolicy(domain, finance, "{\"type\":\"TAG_VALUE\",\"value\":\"finance\"}");
    assignTag(
        catalog.nameIdentifier(), Entity.EntityType.CATALOG, TagValue.of("domain", "finance"));
    assignTag(table.nameIdentifier(), Entity.EntityType.TABLE, TagValue.of("domain", "risk"));

    Assertions.assertNull(
        PolicyMetaService.getInstance()
            .resolveNearestPolicyIds(List.of(table.id()), Policy.BuiltInType.ICEBERG_COMPACTION)
            .get(table.id()));
  }

  @TestTemplate
  public void testSameLevelAmbiguityThrows() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, "cat");
    createAndInsertSchema(METALAKE, "cat", "schema");
    TableEntity table =
        createAndInsertTableEntity(NamespaceUtil.ofTable(METALAKE, "cat", "schema"), "t1");

    PolicyEntity a = insertCompaction("a");
    PolicyEntity b = insertCompaction("b");
    TagEntity tagA = insertTag("tag_a");
    TagEntity tagB = insertTag("tag_b");
    linkPolicy(tagA, a, null);
    linkPolicy(tagB, b, null);
    assignTag(table.nameIdentifier(), Entity.EntityType.TABLE, TagValue.noValue("tag_a"));
    assignTag(table.nameIdentifier(), Entity.EntityType.TABLE, TagValue.noValue("tag_b"));

    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PolicyMetaService.getInstance()
                    .resolveNearestPolicyIds(
                        List.of(table.id()), Policy.BuiltInType.ICEBERG_COMPACTION));
    Assertions.assertTrue(thrown.getMessage().contains("ambiguous"));
  }

  @TestTemplate
  public void testSelectorAndDisabledPolicy() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, "cat");
    createAndInsertSchema(METALAKE, "cat", "schema");
    TableEntity table =
        createAndInsertTableEntity(NamespaceUtil.ofTable(METALAKE, "cat", "schema"), "t1");

    PolicyEntity finance = insertCompaction("finance_policy");
    PolicyEntity disabled = insertCompaction("disabled_policy");
    PolicyEntity disabledStored =
        PolicyEntity.builder()
            .withId(disabled.id())
            .withName(disabled.name())
            .withNamespace(disabled.namespace())
            .withComment(disabled.comment())
            .withPolicyType(disabled.policyType())
            .withEnabled(false)
            .withContent(disabled.content())
            .withAuditInfo(AUDIT_INFO)
            .build();
    PolicyMetaService.getInstance().insertPolicy(disabledStored, true);

    TagEntity domain = insertTag("domain");
    linkPolicy(domain, finance, "{\"type\":\"TAG_VALUE\",\"value\":\"finance\"}");
    linkPolicy(domain, disabledStored, null);
    assignTag(table.nameIdentifier(), Entity.EntityType.TABLE, TagValue.of("domain", "finance"));

    Assertions.assertEquals(
        finance.id(),
        PolicyMetaService.getInstance()
            .resolveNearestPolicyIds(List.of(table.id()), Policy.BuiltInType.ICEBERG_COMPACTION)
            .get(table.id()));

    TagMetaService.getInstance()
        .associateTagValuesWithMetadataObject(
            table.nameIdentifier(),
            Entity.EntityType.TABLE,
            new TagValue[] {TagValue.of("domain", "engineering")},
            new TagValue[] {TagValue.of("domain", "finance")});
    Assertions.assertNull(
        PolicyMetaService.getInstance()
            .resolveNearestPolicyIds(List.of(table.id()), Policy.BuiltInType.ICEBERG_COMPACTION)
            .get(table.id()));
  }

  private PolicyEntity insertCompaction(String name) throws IOException {
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withEnabled(true)
            .withContent(PolicyContents.icebergDataCompaction())
            .withAuditInfo(AUDIT_INFO)
            .build();
    PolicyMetaService.getInstance().insertPolicy(policy, false);
    return policy;
  }

  private TagEntity insertTag(String name) throws IOException {
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withNamespace(NamespaceUtil.ofTag(METALAKE))
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(tag, false);
    return tag;
  }

  private void linkPolicy(TagEntity tag, PolicyEntity policy, String selector) throws IOException {
    backend.updateEntityRelations(
        RelationUpdate.of(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            new RelationEdgeTarget[] {
              RelationEdgeTarget.of(policy.nameIdentifier(), Entity.EntityType.POLICY, selector)
            },
            new RelationEdgeTarget[0]));
  }

  private void assignTag(
      org.apache.gravitino.NameIdentifier objectIdent, Entity.EntityType type, TagValue tagValue)
      throws IOException {
    TagMetaService.getInstance()
        .associateTagValuesWithMetadataObject(
            objectIdent, type, new TagValue[] {tagValue}, new TagValue[0]);
  }
}
