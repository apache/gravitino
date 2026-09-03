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

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
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

  private SupportsRelationOperations relationOperations;
  private EffectiveTagResolver resolver;
  private MockedStatic<MetadataObjectUtil> metadataObjectUtil;

  @BeforeEach
  void setUp() {
    EntityStore entityStore = mock(EntityStore.class);
    relationOperations = mock(SupportsRelationOperations.class);
    when(entityStore.relationOperations()).thenReturn(relationOperations);
    resolver = new EffectiveTagResolver(entityStore);
    metadataObjectUtil = mockStatic(MetadataObjectUtil.class, CALLS_REAL_METHODS);
    metadataObjectUtil
        .when(() -> MetadataObjectUtil.checkMetadataObject(METALAKE, TABLE))
        .thenAnswer(invocation -> null);
  }

  @AfterEach
  void tearDown() {
    metadataObjectUtil.close();
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
