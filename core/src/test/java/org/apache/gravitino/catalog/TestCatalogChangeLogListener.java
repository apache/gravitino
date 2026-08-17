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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
import org.mockito.MockedStatic;

public class TestCatalogChangeLogListener {

  @Test
  @SuppressWarnings("unchecked")
  void testFailedClearPropagatesToThePoller() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier failing = NameIdentifier.of("metalake", "failing");
    NameIdentifier laterLocal = NameIdentifier.of("metalake", "later_local");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(failing)).thenReturn(false);
    when(catalogManager.consumeLocalMutation(laterLocal)).thenReturn(true);
    doThrow(new RuntimeException("eviction failed")).when(catalogCache).invalidate(failing);
    doThrow(new RuntimeException("clear failed")).when(catalogCache).invalidateAll();

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    // There is nothing else we can do here, so let the exception go up. That is fine now: the
    // poller only logs it and never sends the batch again. Re-sending was the dangerous part,
    // because consumeLocalMutation() works only once and a second pass would mistake a change made
    // by this node for one made by another node.
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            listener.onEntityChange(
                List.of(change(1L, "metalake.failing"), change(2L, "metalake.later_local"))));

    // The "made by this node" marks are all read before any removal starts, even when both the
    // removal and the clear fail.
    verify(catalogManager).consumeLocalMutation(laterLocal);
    verify(catalogCache).invalidateAll();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testFailedEvictionClearsTheWholeCatalogCache() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier failing = NameIdentifier.of("metalake", "failing");
    NameIdentifier laterRemote = NameIdentifier.of("metalake", "later_remote");
    NameIdentifier laterLocal = NameIdentifier.of("metalake", "later_local");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(failing)).thenReturn(false);
    when(catalogManager.consumeLocalMutation(laterRemote)).thenReturn(false);
    when(catalogManager.consumeLocalMutation(laterLocal)).thenReturn(true);
    doThrow(new RuntimeException("eviction failed")).when(catalogCache).invalidate(failing);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    Assertions.assertDoesNotThrow(
        () ->
            listener.onEntityChange(
                List.of(
                    change(1L, "metalake.failing"),
                    change(2L, "metalake.later_remote"),
                    change(3L, "metalake.later_local"))));

    // The later local mark is read before any removal starts. Clearing the whole cache then covers
    // both the removal that failed and every remote record, so nothing else has to be removed.
    verify(catalogManager).consumeLocalMutation(laterLocal);
    verify(catalogCache).invalidateAll();
    verify(catalogCache, never()).invalidate(laterRemote);
    verify(catalogCache, never()).invalidate(laterLocal);
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
    // Clearing closes IsolatedClassLoaders that are still in use, so it must never happen during
    // normal operation.
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

    // A row that does not point at any catalog leaves nothing stale, so it must not cause a
    // whole-cache clear.
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
  void testFailedLocalMutationProbeTreatsTheRecordAsRemote() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier failing = NameIdentifier.of("metalake", "failing");
    NameIdentifier healthy = NameIdentifier.of("metalake", "healthy");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(failing))
        .thenThrow(new RuntimeException("probe failed"));
    when(catalogManager.consumeLocalMutation(healthy)).thenReturn(false);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    // When we cannot tell which node made the change, assume another node did. Removing one entry
    // for nothing is cheaper than missing a real remote change, which we would never see again.
    Assertions.assertDoesNotThrow(
        () ->
            listener.onEntityChange(
                List.of(change(1L, "metalake.failing"), change(2L, "metalake.healthy"))));

    verify(catalogCache).invalidate(failing);
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

  @Test
  @SuppressWarnings("unchecked")
  void testUnexpectedDecodeFailureSkipsOnlyThatRecord() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier healthy = NameIdentifier.of("metalake", "healthy");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(any())).thenReturn(false);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);

    // The codec throws IllegalArgumentException today, but that is an implementation detail two
    // calls down. If it ever throws something else, one bad row must still be skipped instead of
    // aborting the batch and dropping the invalidations already collected for the other rows.
    try (MockedStatic<EntityChangeLogNameIdentifierCodec> codec =
        mockStatic(EntityChangeLogNameIdentifierCodec.class, CALLS_REAL_METHODS)) {
      codec
          .when(() -> EntityChangeLogNameIdentifierCodec.decode("metalake.boom"))
          .thenThrow(new IllegalStateException("codec blew up"));

      Assertions.assertDoesNotThrow(
          () ->
              listener.onEntityChange(
                  List.of(change(1L, "metalake.boom"), change(2L, "metalake.healthy"))));
    }

    verify(catalogCache).invalidate(healthy);
    verify(catalogCache, never()).invalidateAll();
  }

  private static EntityChangeRecord change(long id, String fullName) {
    return new EntityChangeRecord(id, "metalake", "CATALOG", fullName, OperateType.ALTER, 0L);
  }
}
