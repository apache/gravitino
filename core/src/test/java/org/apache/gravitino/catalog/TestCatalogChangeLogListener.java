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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
  void testFailedClearPropagatesToThePoller() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier ident = NameIdentifier.of("metalake", "cat");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(ident)).thenReturn(false);
    doThrow(new RuntimeException("eviction failed")).when(catalogCache).invalidate(ident);
    doThrow(new RuntimeException("clear failed")).when(catalogCache).invalidateAll();

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    // Nothing is left to try locally. Propagating is safe now that the poller only logs a listener
    // failure: it never replays the batch, which is what used to be dangerous here, because
    // consumeLocalMutation() is single-shot and a replay would classify a local mutation as remote.
    Assertions.assertThrows(
        RuntimeException.class, () -> listener.onEntityChange(List.of(change(1L, "metalake.cat"))));

    verify(catalogCache).invalidateAll();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testFailedEvictionClearsTheWholeCatalogCache() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier failing = NameIdentifier.of("metalake", "failing");
    NameIdentifier later = NameIdentifier.of("metalake", "later");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(any())).thenReturn(false);
    doThrow(new RuntimeException("eviction failed")).when(catalogCache).invalidate(failing);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    Assertions.assertDoesNotThrow(
        () ->
            listener.onEntityChange(
                List.of(change(1L, "metalake.failing"), change(2L, "metalake.later"))));

    // The clear is a superset of the failed eviction and of the rest of the batch, so the remaining
    // records need no separate eviction.
    verify(catalogCache).invalidateAll();
    verify(catalogCache, never()).invalidate(later);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testSuccessfulBatchDoesNotClearTheCatalogCache() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier first = NameIdentifier.of("metalake", "cat1");
    NameIdentifier second = NameIdentifier.of("metalake", "cat2");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(any())).thenReturn(false);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    listener.onEntityChange(List.of(change(1L, "metalake.cat1"), change(2L, "metalake.cat2")));

    verify(catalogCache).invalidate(first);
    verify(catalogCache).invalidate(second);
    // Clearing closes in-use IsolatedClassLoaders, so it must stay off the normal path.
    verify(catalogCache, never()).invalidateAll();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testMalformedRecordIsSkippedWithoutClearingTheCatalogCache() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier healthy = NameIdentifier.of("metalake", "healthy");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(any())).thenReturn(false);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    // A row that names no catalog leaves nothing stale, so it must not escalate to a clear.
    Assertions.assertDoesNotThrow(
        () ->
            listener.onEntityChange(
                List.of(
                    new EntityChangeRecord(1L, "metalake", "CATALOG", null, OperateType.ALTER, 0L),
                    change(2L, "metalake.cat.schema"),
                    change(3L, "metalake.healthy"))));

    verify(catalogCache).invalidate(healthy);
    verify(catalogCache, never()).invalidateAll();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testFailedLocalMutationProbeSkipsTheRecordWithoutClearing() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier failing = NameIdentifier.of("metalake", "failing");
    NameIdentifier healthy = NameIdentifier.of("metalake", "healthy");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(failing))
        .thenThrow(new RuntimeException("probe failed"));
    when(catalogManager.consumeLocalMutation(healthy)).thenReturn(false);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    // Without a dedup answer there is no eviction to recover, so the record is skipped rather than
    // tearing down every cached catalog over a bookkeeping failure.
    Assertions.assertDoesNotThrow(
        () ->
            listener.onEntityChange(
                List.of(change(1L, "metalake.failing"), change(2L, "metalake.healthy"))));

    verify(catalogCache, never()).invalidate(failing);
    verify(catalogCache).invalidate(healthy);
    verify(catalogCache, never()).invalidateAll();
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
