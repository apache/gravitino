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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

  @BeforeEach
  void setUp() {
    entityStore = mock(EntityStore.class);
    relationOperations = mock(SupportsRelationOperations.class);
    when(entityStore.relationOperations()).thenReturn(relationOperations);
    resolver = new EffectiveTagResolver(entityStore);
    metadataObjectUtil = mockStatic(MetadataObjectUtil.class, CALLS_REAL_METHODS);
    metadataObjectUtil
        .when(() -> MetadataObjectUtil.checkMetadataObject(METALAKE, TABLE))
        .thenAnswer(invocation -> null);
    metadataObjectUtil
        .when(() -> MetadataObjectUtil.checkMetadataObjects(eq(METALAKE), anyList()))
        .thenAnswer(invocation -> null);
  }

  @AfterEach
  void tearDown() {
    metadataObjectUtil.close();
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
