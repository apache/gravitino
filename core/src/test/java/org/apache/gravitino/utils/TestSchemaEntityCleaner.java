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
package org.apache.gravitino.utils;

import static org.apache.gravitino.Entity.EntityType.SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.storage.SupportsIdentityFencedDelete;
import org.junit.jupiter.api.Test;

/** Tests that orphan cleanup only removes the schema registration it observed. */
public class TestSchemaEntityCleaner {

  @Test
  public void testRecreatedSchemaSurvivesCleanup() throws IOException {
    NameIdentifier schema = NameIdentifier.of("metalake", "catalog", "schema");
    Long oldRegistration = 1L;
    Long newRegistration = 2L;
    AtomicReference<Long> current = new AtomicReference<>(oldRegistration);
    EntityStore store =
        mock(EntityStore.class, withSettings().extraInterfaces(SupportsIdentityFencedDelete.class));
    SupportsIdentityFencedDelete fence = (SupportsIdentityFencedDelete) store;
    when(fence.getEntityId(schema, SCHEMA)).thenAnswer(ignored -> current.get());
    when(fence.deleteIfIdMatches(eq(schema), eq(SCHEMA), eq(true), anyLong()))
        .thenAnswer(
            invocation -> {
              Long expected = invocation.getArgument(3);
              if (current.get().longValue() != expected.longValue()) {
                throw new OptimisticLockException("Schema was recreated");
              }
              current.set(null);
              return true;
            });

    SchemaEntityCleaner.deleteOrphanedSchemaEntities(
        store,
        schema,
        true,
        ignored -> {
          current.set(newRegistration);
          return false;
        });

    assertEquals(newRegistration, current.get());
    verify(fence).deleteIfIdMatches(schema, SCHEMA, true, oldRegistration);
  }

  @Test
  public void testUnchangedOrphanIsDeleted() throws IOException {
    NameIdentifier schema = NameIdentifier.of("metalake", "catalog", "schema");
    Long observed = 1L;
    AtomicReference<Long> current = new AtomicReference<>(observed);
    EntityStore store =
        mock(EntityStore.class, withSettings().extraInterfaces(SupportsIdentityFencedDelete.class));
    SupportsIdentityFencedDelete fence = (SupportsIdentityFencedDelete) store;
    when(fence.getEntityId(schema, SCHEMA)).thenReturn(observed);
    when(fence.deleteIfIdMatches(schema, SCHEMA, true, observed))
        .thenAnswer(
            ignored -> {
              current.set(null);
              return true;
            });

    SchemaEntityCleaner.deleteOrphanedSchemaEntities(store, schema, true, ignored -> false);

    assertNull(current.get());
    verify(fence).deleteIfIdMatches(schema, SCHEMA, true, observed);
  }

  @Test
  public void testHierarchicalCleanupDeletesOutermostOrphanWithItsObservation() throws IOException {
    NameIdentifier leaf = NameIdentifier.of("metalake", "catalog", "a:b:c:d");
    NameIdentifier inner = NameIdentifier.of("metalake", "catalog", "a:b:c");
    NameIdentifier outermostOrphan = NameIdentifier.of("metalake", "catalog", "a:b");
    NameIdentifier existingAncestor = NameIdentifier.of("metalake", "catalog", "a");
    Long innerObserved = 1L;
    Long outerObserved = 2L;
    EntityStore store =
        mock(EntityStore.class, withSettings().extraInterfaces(SupportsIdentityFencedDelete.class));
    SupportsIdentityFencedDelete fence = (SupportsIdentityFencedDelete) store;
    when(fence.getEntityId(inner, SCHEMA)).thenReturn(innerObserved);
    when(fence.getEntityId(outermostOrphan, SCHEMA)).thenReturn(outerObserved);

    SchemaEntityCleaner.deleteOrphanedSchemaEntities(
        store, leaf, false, candidate -> existingAncestor.equals(candidate));

    verify(fence).deleteIfIdMatches(outermostOrphan, SCHEMA, true, outerObserved);
    verify(fence, times(1))
        .deleteIfIdMatches(any(NameIdentifier.class), eq(SCHEMA), eq(true), anyLong());
    verify(fence, never()).getEntityId(leaf, SCHEMA);
  }

  @Test
  public void testHierarchicalCleanupSkipsOutermostOrphanWithoutStoreRow() throws IOException {
    NameIdentifier leaf = NameIdentifier.of("metalake", "catalog", "a:b:c");
    NameIdentifier inner = NameIdentifier.of("metalake", "catalog", "a:b");
    NameIdentifier outermostOrphan = NameIdentifier.of("metalake", "catalog", "a");
    EntityStore store =
        mock(EntityStore.class, withSettings().extraInterfaces(SupportsIdentityFencedDelete.class));
    SupportsIdentityFencedDelete fence = (SupportsIdentityFencedDelete) store;
    when(fence.getEntityId(inner, SCHEMA)).thenReturn(1L);
    when(fence.getEntityId(outermostOrphan, SCHEMA))
        .thenThrow(new NoSuchEntityException("No registration for outermost orphan"));

    SchemaEntityCleaner.deleteOrphanedSchemaEntities(store, leaf, false, ignored -> false);

    verify(fence).getEntityId(inner, SCHEMA);
    verify(fence).getEntityId(outermostOrphan, SCHEMA);
    verifyNoMoreInteractions(store);
  }
}
