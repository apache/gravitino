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
package org.apache.gravitino.tag;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;

public class TestEffectiveTagResolver {
  private static final String METALAKE = "metalake";
  private static final MetadataObject TABLE =
      MetadataObjects.of(Arrays.asList("catalog", "schema", "table"), MetadataObject.Type.TABLE);
  private static final MetadataObject SCHEMA =
      MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA);
  private static final MetadataObject CATALOG =
      MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG);

  private EntityStore entityStore;
  private SupportsRelationOperations relationOperations;
  private EffectiveTagResolver resolver;
  private MockedStatic<MetadataObjectUtil> metadataObjectUtil;
  private CatalogManager previousCatalogManager;
  private BaseCatalog<?> backendCatalog;

  @BeforeEach
  void setUp() throws Exception {
    entityStore = mock(EntityStore.class);
    when(entityStore.get(any(), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenThrow(new NoSuchEntityException("missing stored table"));
    relationOperations = mock(SupportsRelationOperations.class);
    when(entityStore.relationOperations()).thenReturn(relationOperations);
    resolver = new EffectiveTagResolver(entityStore);
    previousCatalogManager =
        (CatalogManager) FieldUtils.readField(GravitinoEnv.getInstance(), "catalogManager", true);
    CatalogManager catalogManager = mock(CatalogManager.class);
    backendCatalog = mock(BaseCatalog.class);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, backendCatalog);
    when(backendCatalog.capability()).thenReturn(Capability.DEFAULT);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", catalogManager, true);
    metadataObjectUtil = mockStatic(MetadataObjectUtil.class, CALLS_REAL_METHODS);
    metadataObjectUtil
        .when(() -> MetadataObjectUtil.checkMetadataObject(eq(METALAKE), any(MetadataObject.class)))
        .thenAnswer(invocation -> null);
    metadataObjectUtil
        .when(() -> MetadataObjectUtil.checkMetadataObjects(eq(METALAKE), anyList()))
        .thenAnswer(invocation -> null);
  }

  @AfterEach
  void tearDown() throws IllegalAccessException {
    metadataObjectUtil.close();
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "catalogManager", previousCatalogManager, true);
  }

  @Test
  void testDisplayReadInheritsFromRegisteredAncestorsWithoutLiveValidation() throws Exception {
    TagEntity inherited = tag(1L, "domain", TagAssignment.ofValues("finance"));
    when(entityStore.exists(
            MetadataObjectUtil.toEntityIdent(METALAKE, SCHEMA), Entity.EntityType.SCHEMA))
        .thenReturn(true);
    NameIdentifier schemaIdent = MetadataObjectUtil.toEntityIdent(METALAKE, SCHEMA);
    RelationalEntity<?> relation =
        new RelationalEntity<>(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            schemaIdent,
            Entity.EntityType.SCHEMA,
            inherited);
    when(relationOperations.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            List.of(schemaIdent),
            Entity.EntityType.SCHEMA))
        .thenReturn(List.of(relation));
    metadataObjectUtil
        .when(() -> MetadataObjectUtil.checkMetadataObjects(eq(METALAKE), anyList()))
        .thenThrow(new NoSuchMetadataObjectException("external table is unavailable"));

    Map<MetadataObject, EffectiveTagResolver.ResolvedTag[]> result =
        resolver.resolveWithSourcesForDisplay(METALAKE, List.of(TABLE, SCHEMA));

    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals(1, result.get(TABLE).length);
    Assertions.assertEquals(SCHEMA, result.get(TABLE)[0].source());
    Assertions.assertArrayEquals(
        new String[] {"finance"}, result.get(TABLE)[0].tag().assignment().orElseThrow().values());
    Assertions.assertTrue(result.get(TABLE)[0].tag().inherited().orElseThrow());
    Assertions.assertFalse(result.get(SCHEMA)[0].tag().inherited().orElseThrow());
    metadataObjectUtil.verify(
        () -> MetadataObjectUtil.checkMetadataObjects(any(), anyList()), never());
    verify(relationOperations, times(1)).batchListEntitiesByRelation(any(), anyList(), any());
  }

  @Test
  void testDisplayReadOfUnregisteredHierarchyReturnsEmptyWithoutLiveValidation() {
    metadataObjectUtil
        .when(() -> MetadataObjectUtil.checkMetadataObjects(eq(METALAKE), anyList()))
        .thenThrow(new NoSuchMetadataObjectException("external table is unavailable"));

    Map<MetadataObject, EffectiveTagResolver.ResolvedTag[]> result =
        resolver.resolveWithSourcesForDisplay(METALAKE, List.of(TABLE));

    Assertions.assertEquals(0, result.get(TABLE).length);
    verifyNoInteractions(relationOperations);
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> resolver.resolveWithSources(METALAKE, List.of(TABLE)));
  }

  @Test
  void testDisplayReadDoesNotHideStoreFailure() throws Exception {
    IOException failure = new IOException("store unavailable");
    when(entityStore.exists(
            MetadataObjectUtil.toEntityIdent(METALAKE, TABLE), Entity.EntityType.TABLE))
        .thenThrow(failure);

    RuntimeException actual =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> resolver.resolveWithSourcesForDisplay(METALAKE, List.of(TABLE)));
    Assertions.assertSame(failure, actual.getCause());
    verifyNoInteractions(relationOperations);
  }

  @Test
  void testDisplayReadRejectsMalformedColumnIdentifier() {
    MetadataObject malformed = mock(MetadataObject.class);
    when(malformed.type()).thenReturn(MetadataObject.Type.COLUMN);
    when(malformed.fullName()).thenReturn("catalog.schema.column");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> resolver.resolveWithSourcesForDisplay(METALAKE, List.of(malformed)));
    verifyNoInteractions(entityStore);
  }

  @Test
  void testDisplayReadRejectsAnotherMetalake() {
    MetadataObject other = MetadataObjects.parse("another_metalake", MetadataObject.Type.METALAKE);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> resolver.resolveWithSourcesForDisplay(METALAKE, List.of(other)));
    verifyNoInteractions(entityStore);
  }

  @Test
  void testDisplayReadRejectsMalformedTableIdentifier() {
    MetadataObject malformed = mock(MetadataObject.class);
    when(malformed.type()).thenReturn(MetadataObject.Type.TABLE);
    when(malformed.fullName()).thenReturn("table");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> resolver.resolveWithSourcesForDisplay(METALAKE, List.of(malformed)));
    verifyNoInteractions(entityStore);
  }

  @Test
  void testDisplayReadOfEmptyListDoesNotAccessStore() {
    Assertions.assertTrue(resolver.resolveWithSourcesForDisplay(METALAKE, List.of()).isEmpty());
    verifyNoInteractions(entityStore);
  }

  @Test
  void testNearestAssignmentOverridesAncestorAndOrderIsDeterministic() throws Exception {
    TagEntity directDomain = tag(1L, "domain", TagAssignment.ofValues("finance"));
    TagEntity schemaDomain = tag(2L, "domain", TagAssignment.ofValues("risk"));
    TagEntity schemaClassification =
        tag(3L, "classification", TagAssignment.ofValues("confidential"));
    TagEntity catalogOwner = tag(4L, "owner", TagAssignment.ofValues("data-platform"));
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, TABLE),
            Entity.EntityType.TABLE))
        .thenReturn(Collections.singletonList(directDomain));
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, SCHEMA),
            Entity.EntityType.SCHEMA))
        .thenReturn(Arrays.asList(schemaDomain, schemaClassification));
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, CATALOG),
            Entity.EntityType.CATALOG))
        .thenReturn(Collections.singletonList(catalogOwner));

    TagEntity[] effectiveTags = resolver.resolve(METALAKE, TABLE);

    Assertions.assertArrayEquals(
        new TagEntity[] {directDomain, schemaClassification, catalogOwner}, effectiveTags);
    Assertions.assertArrayEquals(
        new String[] {"finance"}, effectiveTags[0].assignment().orElseThrow().values());
    Assertions.assertFalse(effectiveTags[0].inherited().orElseThrow());
    Assertions.assertTrue(effectiveTags[1].inherited().orElseThrow());
    Assertions.assertTrue(effectiveTags[2].inherited().orElseThrow());
  }

  @Test
  void testResolveWithSourcesPreservesContributingObject() throws Exception {
    TagEntity directDomain = tag(1L, "domain", TagAssignment.ofValues("risk"));
    TagEntity catalogDomain = tag(2L, "domain", TagAssignment.ofValues("finance"));
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, TABLE),
            Entity.EntityType.TABLE))
        .thenReturn(Collections.singletonList(directDomain));
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, SCHEMA),
            Entity.EntityType.SCHEMA))
        .thenReturn(Collections.emptyList());
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, CATALOG),
            Entity.EntityType.CATALOG))
        .thenReturn(Collections.singletonList(catalogDomain));

    EffectiveTagResolver.ResolvedTag[] effectiveTags = resolver.resolveWithSources(METALAKE, TABLE);

    Assertions.assertEquals(1, effectiveTags.length);
    Assertions.assertEquals(TABLE, effectiveTags[0].source());
    Assertions.assertArrayEquals(
        new String[] {"risk"}, effectiveTags[0].tag().assignment().orElseThrow().values());
    Assertions.assertFalse(effectiveTags[0].tag().inherited().orElseThrow());
  }

  @Test
  void testRequestResolutionSharesAncestorTagsForMissingTarget() throws Exception {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    NameIdentifier schema = MetadataObjectUtil.toEntityIdent(METALAKE, SCHEMA);
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, TABLE),
            Entity.EntityType.TABLE))
        .thenThrow(new NoSuchEntityException("missing table"));
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            schema,
            Entity.EntityType.SCHEMA))
        .thenReturn(List.of(tag(1L, "tier", TagAssignment.ofValues("gold"))));

    Assertions.assertEquals(
        SCHEMA, resolver.resolveWithSources(METALAKE, TABLE, context)[0].source());
    Assertions.assertEquals(
        SCHEMA, resolver.resolveWithSources(METALAKE, SCHEMA, context)[0].source());
    verify(relationOperations)
        .listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            schema,
            Entity.EntityType.SCHEMA);
  }

  @Test
  void testMissingObjectIsTranslated() throws Exception {
    NoSuchEntityException exception = new NoSuchEntityException("missing");
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            MetadataObjectUtil.toEntityIdent(METALAKE, TABLE),
            Entity.EntityType.TABLE))
        .thenThrow(exception);

    NoSuchMetadataObjectException actual =
        Assertions.assertThrows(
            NoSuchMetadataObjectException.class, () -> resolver.resolve(METALAKE, TABLE));
    Assertions.assertSame(exception, actual.getCause());
  }

  @Test
  void testBatchSharesAncestorsAndPreservesNearestAssignment() throws Exception {
    MetadataObject first =
        MetadataObjects.of(
            Arrays.asList("catalog", "schema", "table", "first"), MetadataObject.Type.COLUMN);
    MetadataObject second =
        MetadataObjects.of(
            Arrays.asList("catalog", "schema", "table", "second"), MetadataObject.Type.COLUMN);
    TagEntity inherited = tag(1L, "domain", TagAssignment.ofValues("finance"));
    TagEntity direct = tag(1L, "domain", TagAssignment.ofValues("engineering"));
    when(relationOperations.batchListEntitiesByRelation(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Entity.EntityType type = invocation.getArgument(2);
              if (type == Entity.EntityType.TABLE) {
                return Collections.singletonList(
                    new RelationalEntity<>(
                        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                        MetadataObjectUtil.toEntityIdent(METALAKE, TABLE),
                        type,
                        inherited));
              }
              if (type == Entity.EntityType.COLUMN) {
                return Collections.singletonList(
                    new RelationalEntity<>(
                        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                        MetadataObjectUtil.toEntityIdent(METALAKE, first),
                        type,
                        direct));
              }
              return Collections.emptyList();
            });

    Map<MetadataObject, TagEntity[]> result = resolver.resolve(METALAKE, List.of(first, second));

    metadataObjectUtil.verify(
        () -> MetadataObjectUtil.checkMetadataObjects(METALAKE, List.of(first, second)));

    Assertions.assertArrayEquals(
        new String[] {"engineering"}, result.get(first)[0].assignment().orElseThrow().values());
    Assertions.assertFalse(result.get(first)[0].inherited().orElseThrow());
    Assertions.assertArrayEquals(
        new String[] {"finance"}, result.get(second)[0].assignment().orElseThrow().values());
    Assertions.assertTrue(result.get(second)[0].inherited().orElseThrow());
    verify(relationOperations, times(1))
        .batchListEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            List.of(MetadataObjectUtil.toEntityIdent(METALAKE, TABLE)),
            Entity.EntityType.TABLE);

    Map<MetadataObject, EffectiveTagResolver.ResolvedTag[]> withSources =
        resolver.resolveWithSources(METALAKE, List.of(first, second));
    Assertions.assertEquals(first, withSources.get(first)[0].source());
    Assertions.assertEquals(TABLE, withSources.get(second)[0].source());
    Assertions.assertArrayEquals(
        result.get(first),
        Arrays.stream(withSources.get(first))
            .map(EffectiveTagResolver.ResolvedTag::tag)
            .toArray(TagEntity[]::new));
    Assertions.assertArrayEquals(
        result.get(second),
        Arrays.stream(withSources.get(second))
            .map(EffectiveTagResolver.ResolvedTag::tag)
            .toArray(TagEntity[]::new));
  }

  @ParameterizedTest
  @EnumSource(
      value = Entity.EntityType.class,
      names = {
        "SCHEMA",
        "TABLE",
        "COLUMN",
        "VIEW",
        "FUNCTION",
        "FILESET",
        "TOPIC",
        "MODEL",
        "SEMANTIC_MODEL"
      })
  void testNormalizesObjectAndHierarchicalAncestors(Entity.EntityType type) throws Exception {
    when(backendCatalog.capability()).thenReturn(caseInsensitiveCapability());
    String name = type == Entity.EntityType.SEMANTIC_MODEL ? "SalesModel" : "OBJECT";
    NameIdentifier identifier =
        type == Entity.EntityType.SCHEMA
            ? NameIdentifier.of(METALAKE, "catalog", "PARENT:CHILD")
            : type == Entity.EntityType.COLUMN
                ? NameIdentifier.of(METALAKE, "catalog", "PARENT:CHILD", name, "COLUMN")
                : NameIdentifier.of(METALAKE, "catalog", "PARENT:CHILD", name);
    MetadataObject request = NameIdentifierUtil.toMetadataObject(identifier, type);
    List<MetadataObject> order = new ArrayList<>();
    order.add(request);
    order.addAll(MetadataObjectUtil.getParentMetadataObjects(request));
    Map<NameIdentifier, TagEntity> tagsByIdentifier = new LinkedHashMap<>();
    for (int i = 0; i < order.size(); i++) {
      MetadataObject source = order.get(i);
      String[] levels = MetadataObjectUtil.toEntityIdent(METALAKE, source).namespace().levels();
      for (int j = 2; j < levels.length; j++) {
        levels[j] = levels[j].toLowerCase(Locale.ROOT);
      }
      String storedName =
          source.type() == MetadataObject.Type.SEMANTIC_MODEL
              ? source.name()
              : source.name().toLowerCase(Locale.ROOT);
      NameIdentifier stored = NameIdentifier.of(Namespace.of(levels), storedName);
      TagEntity tag = tag(i + 1, "tag" + i, TagAssignment.noValue());
      tagsByIdentifier.put(stored, tag);
      when(relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              stored,
              MetadataObjectUtil.toEntityType(source)))
          .thenReturn(List.of(tag));
    }
    when(relationOperations.batchListEntitiesByRelation(any(), anyList(), any()))
        .thenAnswer(
            invocation -> {
              List<NameIdentifier> identifiers = invocation.getArgument(1);
              Entity.EntityType entityType = invocation.getArgument(2);
              List<RelationalEntity<?>> relations = new ArrayList<>();
              for (NameIdentifier ident : identifiers) {
                if (tagsByIdentifier.containsKey(ident)) {
                  relations.add(
                      new RelationalEntity<>(
                          SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                          ident,
                          entityType,
                          tagsByIdentifier.get(ident)));
                }
              }
              return relations;
            });

    EffectiveTagResolver.ResolvedTag[] single = resolver.resolveWithSources(METALAKE, request);
    EffectiveTagResolver.ResolvedTag[] batch =
        resolver.resolveWithSources(METALAKE, List.of(request)).get(request);
    Assertions.assertEquals(order.size(), single.length);
    Assertions.assertArrayEquals(single, batch);
    for (int i = 0; i < order.size(); i++) {
      Assertions.assertEquals(order.get(i), single[i].source());
      Assertions.assertEquals(i > 0, single[i].tag().inherited().orElseThrow());
    }
  }

  @Test
  void testBatchSharesNormalizedAncestorsAndPreservesRequestKeys() throws Exception {
    when(backendCatalog.capability()).thenReturn(caseInsensitiveCapability());
    MetadataObject upper =
        MetadataObjects.parse("catalog.SCHEMA.TABLE.FIRST", MetadataObject.Type.COLUMN);
    MetadataObject lower =
        MetadataObjects.parse("catalog.schema.table.first", MetadataObject.Type.COLUMN);
    MetadataObject second =
        MetadataObjects.parse("catalog.schema.table.second", MetadataObject.Type.COLUMN);
    TagEntity inherited = tag(1L, "domain", TagAssignment.ofValues("finance"));
    TagEntity direct = tag(1L, "domain", TagAssignment.ofValues("engineering"));
    when(relationOperations.batchListEntitiesByRelation(any(), anyList(), any()))
        .thenAnswer(
            invocation -> {
              Entity.EntityType type = invocation.getArgument(2);
              if (type == Entity.EntityType.TABLE || type == Entity.EntityType.COLUMN) {
                return List.of(
                    new RelationalEntity<>(
                        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                        MetadataObjectUtil.toEntityIdent(
                            METALAKE, type == Entity.EntityType.TABLE ? TABLE : lower),
                        type,
                        type == Entity.EntityType.TABLE ? inherited : direct));
              }
              return Collections.emptyList();
            });

    List<MetadataObject> requests = List.of(upper, lower, second);
    Map<MetadataObject, EffectiveTagResolver.ResolvedTag[]> result =
        resolver.resolveWithSources(METALAKE, requests);
    Assertions.assertEquals(requests, List.copyOf(result.keySet()));
    for (MetadataObject request : List.of(upper, lower)) {
      Assertions.assertEquals(1, result.get(request).length);
      Assertions.assertEquals(request, result.get(request)[0].source());
      Assertions.assertArrayEquals(
          new String[] {"engineering"},
          result.get(request)[0].tag().assignment().orElseThrow().values());
      Assertions.assertFalse(result.get(request)[0].tag().inherited().orElseThrow());
    }
    Assertions.assertEquals(TABLE, result.get(second)[0].source());
    Assertions.assertArrayEquals(
        new String[] {"finance"}, result.get(second)[0].tag().assignment().orElseThrow().values());
    Assertions.assertTrue(result.get(second)[0].tag().inherited().orElseThrow());
    verify(relationOperations)
        .batchListEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            List.of(MetadataObjectUtil.toEntityIdent(METALAKE, TABLE)),
            Entity.EntityType.TABLE);
    verify(relationOperations)
        .batchListEntitiesByRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            List.of(
                MetadataObjectUtil.toEntityIdent(METALAKE, lower),
                MetadataObjectUtil.toEntityIdent(METALAKE, second)),
            Entity.EntityType.COLUMN);
  }

  private static Capability caseInsensitiveCapability() {
    return new Capability() {
      @Override
      public CapabilityResult caseSensitiveOnName(Scope scope) {
        return CapabilityResult.unsupported("case insensitive");
      }
    };
  }

  private static TagEntity tag(long id, String name, TagAssignment assignment) {
    return TagEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofTag(METALAKE))
        .withProperties(Collections.emptyMap())
        .withAuditInfo(
            AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
        .build()
        .copyWithAssignment(assignment);
  }
}
