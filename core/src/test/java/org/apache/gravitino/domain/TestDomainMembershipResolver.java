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
package org.apache.gravitino.domain;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationQuery;
import org.apache.gravitino.SupportsDomainOperations;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

class TestDomainMembershipResolver {
  private static final String METALAKE = "metalake";
  private static final MetadataObject TABLE = object("cat.db.trades", MetadataObject.Type.TABLE);
  private static final MetadataObject SCHEMA = object("cat.db", MetadataObject.Type.SCHEMA);
  private static final MetadataObject CATALOG = object("cat", MetadataObject.Type.CATALOG);
  private static final AuditInfo AUDIT =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build();

  private CatalogManager previousCatalogManager;
  private EntityStore store;
  private SupportsRelationOperations relations;
  private DomainMembershipResolver resolver;
  private SupportsDomainOperations domainOperations;
  private MockedStatic<MetadataObjectUtil> metadataObjects;
  private final Map<String, List<TagEntity>> assignments = new HashMap<>();
  private final Map<Long, List<GenericEntity>> candidates = new HashMap<>();

  @BeforeEach
  void setUp() throws Exception {
    previousCatalogManager =
        (CatalogManager) FieldUtils.readField(GravitinoEnv.getInstance(), "catalogManager", true);
    CatalogManager catalogManager = mock(CatalogManager.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    when(catalog.capability()).thenReturn(Capability.DEFAULT);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", catalogManager, true);
    store = mock(EntityStore.class);
    relations = mock(SupportsRelationOperations.class);
    when(store.relationOperations()).thenReturn(relations);
    domainOperations = mock(SupportsDomainOperations.class);
    when(store.domainOperations()).thenReturn(domainOperations);
    resolver = new DomainMembershipResolver(store);
    metadataObjects = mockStatic(MetadataObjectUtil.class, CALLS_REAL_METHODS);
    metadataObjects
        .when(() -> MetadataObjectUtil.checkMetadataObject(anyString(), any()))
        .thenAnswer(call -> null);
    when(relations.listEntitiesByRelation(any(RelationQuery.class)))
        .thenAnswer(
            call -> {
              RelationQuery query = call.getArgument(0);
              assertEquals(Entity.EntityType.TAG, query.anchorEntityType());
              assertTrue(query.anchorId().isPresent());
              return candidates.getOrDefault(query.anchorId().get(), Collections.emptyList());
            });
    when(relations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
            any(NameIdentifier.class),
            any(Entity.EntityType.class)))
        .thenAnswer(
            call ->
                assignments.getOrDefault(
                    call.<NameIdentifier>getArgument(1).toString(), Collections.emptyList()));
  }

  @AfterEach
  void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "catalogManager", previousCatalogManager, true);
    metadataObjects.close();
  }

  @Test
  void testAllRelationsAndScopeFilters() {
    DomainEntity parent = domain(1, "Finance", null, 10L, null);
    DomainEntity child = domain(2, "Trading", "Finance", 20L, null);
    DomainEntity second = domain(3, "Tax", "Finance", 30L, null);
    DomainEntity[] domains = {parent, child, second};
    assign(SCHEMA, tag(10, "finance"));
    assign(TABLE, tag(20, "trading"), tag(30, "tax"));
    candidate(10, TABLE);
    candidate(20, TABLE);
    candidate(30, TABLE);
    DomainRelation[] expected = {
      DomainRelation.inherited(SCHEMA),
      DomainRelation.viaSubdomain("Tax"),
      DomainRelation.viaSubdomain("Trading")
    };
    for (DomainMemberFilter filter :
        Arrays.asList(
            all(),
            DomainMemberFilter.builder().withDomains("Finance").build(),
            DomainMemberFilter.builder().withDomains("Trading").build(),
            DomainMemberFilter.builder().withDomains("Tax", "Trading").build())) {
      DomainMember[] members = resolver.listMembers(METALAKE, parent, domains, filter);
      assertEquals(1, members.length);
      assertEquals(TABLE, members[0].object());
      assertArrayEquals(expected, members[0].relations());
    }
    assertEquals(
        0,
        resolver.listMembers(
                METALAKE,
                parent,
                domains,
                DomainMemberFilter.builder().withDomains("missing").build())
            .length);
    assertArrayEquals(
        new DomainRelation[] {DomainRelation.direct()},
        resolver.listMembers(METALAKE, child, domains, all())[0].relations());
    DomainEntity[] reverse = listDomains(METALAKE, TABLE, domains);
    assertEquals(3, reverse.length);
    assertArrayEquals(expected, reverse[0].relations());
    assertEquals(0, parent.relations().length);
  }

  @Test
  void testValueOverrideCaseAndNoValueAssignment() {
    DomainEntity finance = domain(1, "Finance", null, 10L, "finance");
    DomainEntity hr = domain(2, "HR", null, 10L, "hr");
    DomainEntity upper = domain(3, "Upper", null, 10L, "Finance");
    DomainEntity[] domains = {finance, hr, upper};
    candidate(10, TABLE);
    assign(SCHEMA, tag(10, "dept", "finance"));
    assign(TABLE, tag(10, "dept", "hr"));
    assertEquals(0, resolver.listMembers(METALAKE, finance, domains, all()).length);
    assertEquals("HR", listDomains(METALAKE, TABLE, domains)[0].name());
    assign(TABLE, tag(10, "dept", "finance", "hr"));
    assertEquals(2, listDomains(METALAKE, TABLE, domains).length);
    assertEquals(0, resolver.listMembers(METALAKE, upper, domains, all()).length);
    assign(TABLE, tag(10, "dept"));
    assertEquals(0, listDomains(METALAKE, TABLE, domains).length);
    assertEquals(0, resolver.listMembers(METALAKE, finance, domains, all()).length);
    DomainEntity anyValue = domain(4, "Any", null, 10L, null);
    assertEquals(1, listDomains(METALAKE, TABLE, new DomainEntity[] {anyValue}).length);
    assign(TABLE, tag(10, "dept", "other"));
    assertEquals(
        1, resolver.listMembers(METALAKE, anyValue, new DomainEntity[] {anyValue}, all()).length);
  }

  @Test
  void testIdNotNameAndMissingBinding() {
    DomainEntity domain = domain(1, "Finance", null, 10L, null);
    candidate(10, TABLE);
    assign(TABLE, tag(20, "finance"));
    assertEquals(
        0, resolver.listMembers(METALAKE, domain, new DomainEntity[] {domain}, all()).length);
    assertEquals(0, listDomains(METALAKE, TABLE, new DomainEntity[] {domain}).length);
    assign(TABLE, tag(10, "renamed"));
    assertEquals(
        1, resolver.listMembers(METALAKE, domain, new DomainEntity[] {domain}, all()).length);
    DomainEntity unbound = domain(2, "Unbound", null, null, null);
    assertEquals(
        0, resolver.listMembers(METALAKE, unbound, new DomainEntity[] {unbound}, all()).length);
    assertEquals(0, listDomains(METALAKE, TABLE, new DomainEntity[] {unbound}).length);
  }

  @Test
  void testCatalogExpansionAndLeafFilters() throws Exception {
    DomainEntity domain = domain(1, "Finance", null, 10L, null);
    DomainEntity[] domains = {domain};
    candidate(
        10,
        CATALOG,
        SCHEMA,
        TABLE,
        object("cat.db.trades.column", MetadataObject.Type.COLUMN),
        object("cat.db.func", MetadataObject.Type.FUNCTION));
    assign(CATALOG, tag(10, "finance"));
    when(store.list(Namespace.of(METALAKE, "cat"), SchemaEntity.class, Entity.EntityType.SCHEMA))
        .thenReturn(
            Collections.singletonList(
                SchemaEntity.builder()
                    .withId(5L)
                    .withName("db")
                    .withNamespace(Namespace.of(METALAKE, "cat"))
                    .withAuditInfo(AUDIT)
                    .build()));
    when(store.list(
            Namespace.of(METALAKE, "cat", "db"), TableEntity.class, Entity.EntityType.TABLE))
        .thenReturn(
            Collections.singletonList(
                TableEntity.builder()
                    .withId(6L)
                    .withName("trades")
                    .withNamespace(Namespace.of(METALAKE, "cat", "db"))
                    .withAuditInfo(AUDIT)
                    .withColumns(Collections.emptyList())
                    .build()));
    DomainMember[] members =
        resolver.listMembers(
            METALAKE,
            domain,
            domains,
            DomainMemberFilter.builder()
                .withTypes(MetadataObject.Type.TABLE)
                .withNameContains("CAT.DB.TRA")
                .build());
    assertEquals(1, members.length);
    assertArrayEquals(
        new DomainRelation[] {DomainRelation.inherited(CATALOG)}, members[0].relations());
    assertEquals(
        0,
        resolver.listMembers(
                METALAKE,
                domain,
                domains,
                DomainMemberFilter.builder().withNameContains("absent").build())
            .length);
    assertEquals(
        0,
        resolver.listMembers(
                METALAKE,
                domain,
                domains,
                DomainMemberFilter.builder().withTypes(MetadataObject.Type.MODEL).build())
            .length);
  }

  @ParameterizedTest
  @ValueSource(strings = {":", "::"})
  void testHierarchicalSchemaExpansionAndOverride(String separator) throws Exception {
    try (MockedStatic<HierarchicalSchemaUtil> hierarchy =
        mockStatic(HierarchicalSchemaUtil.class, CALLS_REAL_METHODS)) {
      hierarchy.when(HierarchicalSchemaUtil::schemaSeparator).thenReturn(separator);
      MetadataObject ancestor = object("cat.a", MetadataObject.Type.SCHEMA);
      MetadataObject childSchema = object("cat.a" + separator + "b", MetadataObject.Type.SCHEMA);
      MetadataObject childTable =
          object(childSchema.fullName() + ".trades", MetadataObject.Type.TABLE);
      SchemaEntity descendant =
          SchemaEntity.builder()
              .withId(5L)
              .withName(childSchema.name())
              .withNamespace(Namespace.of(METALAKE, "cat"))
              .withAuditInfo(AUDIT)
              .build();
      when(store.listDescendantSchemas(MetadataObjectUtil.toEntityIdent(METALAKE, ancestor)))
          .thenReturn(Collections.singletonList(descendant));
      when(store.list(
              Namespace.of(METALAKE, "cat", childSchema.name()),
              TableEntity.class,
              Entity.EntityType.TABLE))
          .thenReturn(
              Collections.singletonList(
                  TableEntity.builder()
                      .withId(6L)
                      .withName("trades")
                      .withNamespace(Namespace.of(METALAKE, "cat", childSchema.name()))
                      .withAuditInfo(AUDIT)
                      .withColumns(Collections.emptyList())
                      .build()));
      DomainEntity finance = domain(1, "Finance", null, 10L, "finance");
      candidate(10, ancestor);
      assign(ancestor, tag(10, "dept", "finance"));
      DomainMemberFilter filter =
          DomainMemberFilter.builder().withTypes(MetadataObject.Type.TABLE).build();
      DomainMember[] members = resolver.listMembers(METALAKE, finance, new DomainEntity[0], filter);
      assertEquals(1, members.length);
      assertEquals(childTable, members[0].object());
      assertArrayEquals(
          new DomainRelation[] {DomainRelation.inherited(ancestor)}, members[0].relations());
      assertEquals(1, listDomains(METALAKE, childTable, new DomainEntity[] {finance}).length);
      assign(childSchema, tag(10, "dept", "hr"));
      assertEquals(0, resolver.listMembers(METALAKE, finance, new DomainEntity[0], filter).length);
      assertEquals(0, listDomains(METALAKE, childTable, new DomainEntity[] {finance}).length);
    }
  }

  @Test
  void testUnknownScopesReturnEmptyWithoutEnumeratingMembers() {
    DomainEntity finance = domain(1, "Finance", null, 10L, "finance");
    DomainMemberFilter filter =
        DomainMemberFilter.builder().withDomains("unknown", "Secret").build();
    assertEquals(0, resolver.listMembers(METALAKE, finance, new DomainEntity[0], filter).length);
    verifyNoInteractions(relations);
  }

  @Test
  void testSupportedLeafTypesAndReverseRejections() {
    DomainEntity domain = domain(1, "Finance", null, 10L, null);
    DomainEntity[] domains = {domain};
    assign(SCHEMA, tag(10, "finance"));
    for (MetadataObject.Type type : DomainMember.SUPPORTED_TYPES) {
      MetadataObject leaf = object("cat.db.asset", type);
      candidate(10, leaf);
      assertEquals(1, resolver.listMembers(METALAKE, domain, domains, all()).length);
      assertEquals(1, listDomains(METALAKE, leaf, domains).length);
    }
    for (MetadataObject object :
        Arrays.asList(
            CATALOG,
            SCHEMA,
            object("cat.db.func", MetadataObject.Type.FUNCTION),
            object("Finance", MetadataObject.Type.DOMAIN))) {
      assertThrows(IllegalArgumentException.class, () -> listDomains(METALAKE, object, domains));
    }
    assertThrows(IllegalArgumentException.class, () -> listDomains(METALAKE, null, domains));
  }

  @Test
  void testDroppedCandidateAndStorageFailure() throws Exception {
    DomainEntity domain = domain(1, "Finance", null, 10L, null);
    candidate(10, TABLE);
    metadataObjects
        .when(() -> MetadataObjectUtil.checkMetadataObject(METALAKE, TABLE))
        .thenThrow(new NoSuchMetadataObjectException("dropped"));
    assertEquals(
        0, resolver.listMembers(METALAKE, domain, new DomainEntity[] {domain}, all()).length);
    assertThrows(
        NoSuchMetadataObjectException.class,
        () -> listDomains(METALAKE, TABLE, new DomainEntity[] {domain}));
    when(relations.listEntitiesByRelation(any(RelationQuery.class)))
        .thenThrow(new IOException("storage"));
    assertInstanceOf(
        IOException.class,
        assertThrows(
                RuntimeException.class,
                () -> resolver.listMembers(METALAKE, domain, new DomainEntity[] {domain}, all()))
            .getCause());
  }

  @Test
  void testReverseLookupSkipsUnboundTagsAndOnlyReadsMatchingParents() throws Exception {
    assertEquals(0, resolver.listDomains(METALAKE, TABLE).length);
    verify(domainOperations, never()).listDomainsByBackingTagIds(anyString(), anyList());
    DomainEntity parent = domain(1, "Finance", null, null, null);
    DomainEntity child = domain(2, "Trading", "Finance", 20L, null);
    DomainEntity unrelated = domain(3, "Unrelated", null, 30L, null);
    assign(TABLE, tag(20, "trading"));
    DomainEntity[] result =
        listDomains(METALAKE, TABLE, new DomainEntity[] {parent, child, unrelated});
    assertEquals(2, result.length);
    verify(domainOperations).listDomainsByBackingTagIds(METALAKE, Collections.singletonList(20L));
    verify(store)
        .batchGet(
            Collections.singletonList(NameIdentifierUtil.ofDomain(METALAKE, "Finance")),
            Entity.EntityType.DOMAIN,
            DomainEntity.class);
    verify(store, never()).list(any(), eq(DomainEntity.class), eq(Entity.EntityType.DOMAIN));
    assertArrayEquals(
        new DomainRelation[] {DomainRelation.viaSubdomain("Trading")}, result[0].relations());
    when(domainOperations.listDomainsByBackingTagIds(anyString(), anyList()))
        .thenThrow(new IOException("failed"));
    assertInstanceOf(
        IOException.class,
        assertThrows(RuntimeException.class, () -> resolver.listDomains(METALAKE, TABLE))
            .getCause());
  }

  @Test
  void testDefensiveRelationCopies() {
    DomainRelation[] relations = {DomainRelation.direct()};
    DomainMemberInfo member = new DomainMemberInfo(TABLE, relations);
    DomainEntity result =
        domain(1, "Finance", null, 10L, null).toBuilder().withRelations(relations).build();
    relations[0] = DomainRelation.inherited(SCHEMA);
    member.relations()[0] = DomainRelation.inherited(CATALOG);
    result.relations()[0] = DomainRelation.inherited(CATALOG);
    assertEquals(DomainRelation.direct(), member.relations()[0]);
    assertEquals(DomainRelation.direct(), result.relations()[0]);
  }

  private DomainEntity[] listDomains(
      String metalake, MetadataObject object, DomainEntity[] domainFixtures) {
    try {
      when(domainOperations.listDomainsByBackingTagIds(eq(metalake), anyList()))
          .thenAnswer(
              call -> {
                List<Long> tagIds = call.getArgument(1);
                return Arrays.stream(domainFixtures)
                    .filter(domain -> tagIds.contains(domain.backingTagId()))
                    .collect(Collectors.toList());
              });
      when(store.batchGet(anyList(), eq(Entity.EntityType.DOMAIN), eq(DomainEntity.class)))
          .thenAnswer(
              call -> {
                List<NameIdentifier> parents = call.getArgument(0);
                return Arrays.stream(domainFixtures)
                    .filter(
                        domain ->
                            parents.stream()
                                .anyMatch(parent -> parent.name().equals(domain.name())))
                    .collect(Collectors.toList());
              });
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    return resolver.listDomains(metalake, object);
  }

  private void assign(MetadataObject object, TagEntity... tags) {
    assignments.put(
        MetadataObjectUtil.toEntityIdent(METALAKE, object).toString(), Arrays.asList(tags));
  }

  private void candidate(long tagId, MetadataObject... objects) {
    List<GenericEntity> entities = new ArrayList<>();
    for (MetadataObject object : objects) {
      entities.add(
          GenericEntity.builder()
              .withId(100L + entities.size())
              .withName(object.fullName())
              .withEntityType(Entity.EntityType.valueOf(object.type().name()))
              .build());
    }
    candidates.put(tagId, entities);
  }

  private static DomainMemberFilter all() {
    return DomainMemberFilter.builder().build();
  }

  private static MetadataObject object(String name, MetadataObject.Type type) {
    return MetadataObjects.parse(name, type);
  }

  private static DomainEntity domain(
      long id, String name, String parent, Long tagId, String value) {
    return DomainEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(Namespace.of(METALAKE))
        .withStatus(Domain.Status.DRAFT)
        .withParent(parent)
        .withBackingTagId(tagId)
        .withBackingTagName(tagId == null ? null : "tag")
        .withBackingTagValue(value)
        .withProperties(Collections.emptyMap())
        .withAuditInfo(AUDIT)
        .build();
  }

  private static TagEntity tag(long id, String name, String... values) {
    return TagEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(Namespace.of(METALAKE))
        .withProperties(Collections.emptyMap())
        .withAuditInfo(AUDIT)
        .withAssignment(
            values.length == 0 ? TagAssignment.noValue() : TagAssignment.ofValues(values))
        .build();
  }
}
