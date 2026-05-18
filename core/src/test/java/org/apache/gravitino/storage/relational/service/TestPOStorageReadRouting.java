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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

import java.util.Collections;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestPOStorageReadRouting {

  private static final Object MAPPER = new Object();

  @Test
  public void getPOUsesFullNameWhenCacheDisabled() {
    RecordingOps ops = new RecordingOps(true);
    NameIdentifier id = NameIdentifier.of("ml", "cat", "schema");
    String result = POStorageReadRouting.getPO(MAPPER, id, ops, Entity.EntityType.SCHEMA, false);
    assertEquals("by-full", result);
    assertFalse(ops.getPOByParentCalled);
    assertTrue(ops.getPOByFullNameCalled);
  }

  @Test
  public void getPOUsesFullNameWhenCacheEnabledButParentPathUnsupported() {
    RecordingOps ops = new RecordingOps(false);
    String result =
        POStorageReadRouting.getPO(
            MAPPER, NameIdentifier.of("ml", "cat", "schema"), ops, Entity.EntityType.SCHEMA, true);
    assertEquals("by-full", result);
    assertFalse(ops.getPOByParentCalled);
    assertTrue(ops.getPOByFullNameCalled);
  }

  @Test
  public void getPOUsesParentIdWhenCacheEnabledAndSupported() {
    try (MockedStatic<EntityIdService> entityIds = mockStatic(EntityIdService.class)) {
      entityIds
          .when(() -> EntityIdService.getEntityId(any(), eq(Entity.EntityType.CATALOG)))
          .thenReturn(42L);
      RecordingOps ops = new RecordingOps(true);
      NameIdentifier id = NameIdentifier.of("ml", "cat", "schema");
      String result = POStorageReadRouting.getPO(MAPPER, id, ops, Entity.EntityType.SCHEMA, true);
      assertEquals("by-parent", result);
      assertTrue(ops.getPOByParentCalled);
      assertFalse(ops.getPOByFullNameCalled);
      assertEquals(Long.valueOf(42), ops.seenParentId);
      assertEquals("schema", ops.seenShortName);
    }
  }

  @Test
  public void listPOsUsesFullNamespaceWhenCacheDisabled() {
    RecordingOps ops = new RecordingOps(true);
    Namespace ns = Namespace.of("ml", "cat", "sch");
    List<String> result =
        POStorageReadRouting.listPOs(MAPPER, ns, ops, Entity.EntityType.TABLE, false);
    assertTrue(result.isEmpty());
    assertFalse(ops.listByParentCalled);
    assertTrue(ops.listByFullNsCalled);
  }

  @Test
  public void listPOsUsesParentIdWhenCacheEnabledAndSupported() {
    try (MockedStatic<EntityIdService> entityIds = mockStatic(EntityIdService.class)) {
      entityIds
          .when(() -> EntityIdService.getEntityId(any(), eq(Entity.EntityType.SCHEMA)))
          .thenReturn(7L);
      RecordingOps ops = new RecordingOps(true);
      Namespace ns = Namespace.of("ml", "cat", "sch");
      List<String> out =
          POStorageReadRouting.listPOs(MAPPER, ns, ops, Entity.EntityType.TABLE, true);
      assertEquals(Collections.singletonList("row"), out);
      assertTrue(ops.listByParentCalled);
      assertFalse(ops.listByFullNsCalled);
      assertEquals(Long.valueOf(7), ops.seenListParentId);
    }
  }

  private static final class RecordingOps extends BasePOStorageOps<String, Object> {
    private final boolean supportsParent;
    boolean getPOByParentCalled;
    boolean getPOByFullNameCalled;
    boolean listByParentCalled;
    boolean listByFullNsCalled;
    Long seenParentId;
    String seenShortName;
    Long seenListParentId;

    RecordingOps(boolean supportsParent) {
      this.supportsParent = supportsParent;
    }

    @Override
    public boolean supportsParentIdRelationalRead() {
      return supportsParent;
    }

    @Override
    public String getPO(Object mapper, Long parentId, String name) {
      getPOByParentCalled = true;
      seenParentId = parentId;
      seenShortName = name;
      return "by-parent";
    }

    @Override
    public String getPOByFullName(Object mapper, NameIdentifier identifier) {
      getPOByFullNameCalled = true;
      return "by-full";
    }

    @Override
    public List<String> listPOs(Object mapper, Long parentId) {
      listByParentCalled = true;
      seenListParentId = parentId;
      return Collections.singletonList("row");
    }

    @Override
    public List<String> listPOsByNSFullName(Object mapper, Namespace namespace) {
      listByFullNsCalled = true;
      return Collections.emptyList();
    }

    @Override
    protected Entity.EntityType entityType() {
      return Entity.EntityType.SCHEMA;
    }
  }
}
