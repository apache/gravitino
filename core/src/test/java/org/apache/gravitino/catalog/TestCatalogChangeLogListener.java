/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager.CatalogWrapper;
import org.apache.gravitino.storage.relational.EntityChangeLogNameIdentifierCodec;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogChangeLogListener {

  @Test
  @SuppressWarnings("unchecked")
  void testProcessesRemainingChangesAndSwallowsFailure() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier failedIdentifier = NameIdentifier.of("metalake", "failed");
    NameIdentifier successfulIdentifier = NameIdentifier.of("metalake", "successful");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(failedIdentifier))
        .thenThrow(new RuntimeException("invalidation failed"));

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    // The failure must not reach the poller. A retried batch would re-invalidate catalogs this
    // process mutated itself, because consumeLocalMutation() is single-shot, and closing an
    // in-use CatalogWrapper also closes its IsolatedClassLoader.
    Assertions.assertDoesNotThrow(
        () ->
            listener.onEntityChange(
                List.of(change(1L, "metalake.failed"), change(2L, "metalake.successful"))));

    verify(catalogCache).invalidate(successfulIdentifier);
    verify(catalogCache, never()).invalidate(failedIdentifier);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testTransientEvictionFailureIsRetriedForTheSameCatalog() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier ident = NameIdentifier.of("metalake", "cat");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(ident)).thenReturn(false);
    doThrow(new RuntimeException("eviction failed"))
        .doNothing()
        .when(catalogCache)
        .invalidate(ident);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    Assertions.assertDoesNotThrow(
        () -> listener.onEntityChange(List.of(change(1L, "metalake.cat"))));

    // The eviction is retried, so a transient failure no longer leaves this catalog stale.
    verify(catalogCache, times(2)).invalidate(ident);
    // The retry must not re-run the single-shot local-mutation probe: doing so would classify a
    // local mutation as remote and close an IsolatedClassLoader this process is still using.
    verify(catalogManager, times(1)).consumeLocalMutation(ident);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testPersistentEvictionFailureIsGivenUpWithoutClearingTheCache() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier failing = NameIdentifier.of("metalake", "failing");
    NameIdentifier healthy = NameIdentifier.of("metalake", "healthy");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(any())).thenReturn(false);
    doThrow(new RuntimeException("eviction failed")).when(catalogCache).invalidate(failing);
    doNothing().when(catalogCache).invalidate(healthy);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    Assertions.assertDoesNotThrow(
        () ->
            listener.onEntityChange(
                List.of(change(1L, "metalake.failing"), change(2L, "metalake.healthy"))));

    // Retries are bounded, the rest of the batch still applies, and the whole cache is NEVER
    // cleared: that would evict catalogs this process is serving and close their in-use
    // IsolatedClassLoaders (#11739).
    verify(catalogCache, times(2)).invalidate(failing);
    verify(catalogCache).invalidate(healthy);
    verify(catalogCache, never()).invalidateAll();
    verify(catalogCache, never()).invalidateAll(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testSkipsLocalMutationAndNonCatalogRecords() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier localIdentifier = NameIdentifier.of("metalake", "local");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(localIdentifier)).thenReturn(true);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    Assertions.assertDoesNotThrow(
        () ->
            listener.onEntityChange(
                List.of(
                    change(1L, "metalake.local"),
                    change(2L, "metalake"),
                    change(3L, "metalake.cat.schema"),
                    new EntityChangeRecord(
                        4L, "metalake", "SCHEMA", "metalake.cat.sch", OperateType.ALTER, 0L))));

    verify(catalogCache, never()).invalidate(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testInvalidatesCatalogWithDotsInsideNameLevels() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier ident = NameIdentifier.of("meta.lake", "cat.alog");
    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);
    listener.onEntityChange(List.of(change(1L, EntityChangeLogNameIdentifierCodec.encode(ident))));

    verify(catalogCache).invalidate(ident);
  }

  private static EntityChangeRecord change(long id, String fullName) {
    return new EntityChangeRecord(id, "metalake", "CATALOG", fullName, OperateType.ALTER, 0L);
  }
}
