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

package org.apache.gravitino.cache;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

public class TestCacheKey {

  private static RandomIdGenerator generator;

  @BeforeAll
  public static void setUp() {
    generator = new RandomIdGenerator();
  }

  @Test
  void testCacheKeyConstructionAndGetters() {
    ModelEntity modelEntity = getTestModelEntity();
    CacheKey cacheKey = new CacheKey(modelEntity);

    Assertions.assertEquals(modelEntity.nameIdentifier(), cacheKey.nameIdent());
    Assertions.assertEquals(modelEntity.id(), cacheKey.id());
    Assertions.assertEquals(Entity.EntityType.MODEL, cacheKey.entityType());

    TableEntity tableEntity = getTestTableEntity();
    cacheKey = new CacheKey(tableEntity);

    Assertions.assertEquals(tableEntity.nameIdentifier(), cacheKey.nameIdent());
    Assertions.assertEquals(tableEntity.id(), cacheKey.id());
    Assertions.assertEquals(Entity.EntityType.TABLE, cacheKey.entityType());
  }

  @Test
  void testEqualsAndHashCodeWithSameValues() {
    ModelEntity modelEntity1 = getTestModelEntity(2L, "test1", Namespace.of("m1", "c1", "s1"));
    ModelEntity modelEntity2 = getTestModelEntity(2L, "test1", modelEntity1.namespace());
    ModelEntity modelEntity3 = getTestModelEntity(1L, "test2", modelEntity1.namespace());
    TableEntity tableEntity = getTestTableEntity();

    CacheKey cacheKey1 = new CacheKey(modelEntity1);
    CacheKey cacheKey2 = new CacheKey(modelEntity2);
    CacheKey cacheKey3 = new CacheKey(modelEntity3);
    CacheKey cacheKey4 = new CacheKey(tableEntity);

    Assertions.assertEquals(cacheKey1, cacheKey2);
    Assertions.assertEquals(cacheKey1.hashCode(), cacheKey2.hashCode());

    Assertions.assertNotEquals(cacheKey1, cacheKey3);
    Assertions.assertNotEquals(cacheKey1.hashCode(), cacheKey3.hashCode());

    Assertions.assertNotEquals(cacheKey2, cacheKey3);
    Assertions.assertNotEquals(cacheKey2.hashCode(), cacheKey3.hashCode());

    Assertions.assertNotEquals(cacheKey1, cacheKey4);
    Assertions.assertNotEquals(cacheKey1.hashCode(), cacheKey4.hashCode());
    Assertions.assertNotEquals(cacheKey2, cacheKey4);
    Assertions.assertNotEquals(cacheKey2.hashCode(), cacheKey4.hashCode());
    Assertions.assertNotEquals(cacheKey3, cacheKey4);
    Assertions.assertNotEquals(cacheKey3.hashCode(), cacheKey4.hashCode());
  }

  private ModelEntity getTestModelEntity() {
    return getTestModelEntity(generator.nextId(), "test", Namespace.of("m1", "c1", "s1"));
  }

  private ModelEntity getTestModelEntity(long id, String name, Namespace namespace) {
    return ModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withLatestVersion(1)
        .withAuditInfo(getTestAuditInfo())
        .build();
  }

  private TableEntity getTestTableEntity() {
    return getTestTableEntity(generator.nextId(), "test", Namespace.of("m1", "c1", "s1"));
  }

  private TableEntity getTestTableEntity(long id, String name, Namespace namespace) {
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withAuditInfo(getTestAuditInfo())
        .withNamespace(namespace)
        .withColumns(ImmutableList.of(getMockColumnEntity()))
        .build();
  }

  private AuditInfo getTestAuditInfo() {
    return AuditInfo.builder()
        .withCreator("admin")
        .withCreateTime(Instant.now())
        .withLastModifier("admin")
        .withLastModifiedTime(Instant.now())
        .build();
  }

  private ColumnEntity getMockColumnEntity() {
    ColumnEntity mockColumn = mock(ColumnEntity.class);
    when(mockColumn.name()).thenReturn("filed1");
    when(mockColumn.dataType()).thenReturn(Types.StringType.get());
    when(mockColumn.nullable()).thenReturn(false);
    when(mockColumn.auditInfo()).thenReturn(getTestAuditInfo());

    return mockColumn;
  }
}
