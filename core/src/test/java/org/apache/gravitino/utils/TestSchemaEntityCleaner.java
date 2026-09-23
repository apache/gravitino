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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.storage.EntityVersion;
import org.junit.jupiter.api.Test;

/** Tests that orphan cleanup only removes the schema registration it observed. */
public class TestSchemaEntityCleaner {

  @Test
  public void testRecreatedSchemaSurvivesCleanup() throws IOException {
    NameIdentifier schema = NameIdentifier.of("metalake", "catalog", "schema");
    EntityVersion oldRegistration = EntityVersion.of(1, 0);
    EntityVersion newRegistration = EntityVersion.of(2, 0);
    AtomicReference<EntityVersion> current = new AtomicReference<>(oldRegistration);
    EntityStore store = mock(EntityStore.class);
    when(store.getVersion(schema, SCHEMA)).thenAnswer(ignored -> current.get());
    when(store.delete(eq(schema), eq(SCHEMA), eq(true), any(EntityVersion.class)))
        .thenAnswer(
            invocation -> {
              EntityVersion expected = invocation.getArgument(3);
              if (current.get().id() != expected.id()) {
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
    verify(store).delete(schema, SCHEMA, true, oldRegistration);
  }

  @Test
  public void testUnchangedOrphanIsDeleted() throws IOException {
    NameIdentifier schema = NameIdentifier.of("metalake", "catalog", "schema");
    EntityVersion observed = EntityVersion.of(1, 0);
    AtomicReference<EntityVersion> current = new AtomicReference<>(observed);
    EntityStore store = mock(EntityStore.class);
    when(store.getVersion(schema, SCHEMA)).thenReturn(observed);
    when(store.delete(schema, SCHEMA, true, observed))
        .thenAnswer(
            ignored -> {
              current.set(null);
              return true;
            });

    SchemaEntityCleaner.deleteOrphanedSchemaEntities(store, schema, true, ignored -> false);

    assertNull(current.get());
    verify(store).delete(schema, SCHEMA, true, observed);
  }
}
