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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager.CatalogWrapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogChangeLogListener {

  @Test
  @SuppressWarnings("unchecked")
  void testProcessesRemainingChangesBeforePropagatingFailure() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Cache<NameIdentifier, CatalogWrapper> catalogCache = mock(Cache.class);
    NameIdentifier failedIdentifier = NameIdentifier.of("metalake", "failed");
    NameIdentifier successfulIdentifier = NameIdentifier.of("metalake", "successful");
    RuntimeException failure = new RuntimeException("invalidation failed");

    when(catalogManager.getCatalogCache()).thenReturn(catalogCache);
    when(catalogManager.consumeLocalMutation(failedIdentifier)).thenThrow(failure);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);
    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                listener.onEntityChange(
                    List.of(change(1L, "metalake.failed"), change(2L, "metalake.successful"))));

    Assertions.assertSame(failure, thrown);
    verify(catalogCache).invalidate(successfulIdentifier);
  }

  private static EntityChangeRecord change(long id, String fullName) {
    return new EntityChangeRecord(id, "metalake", "CATALOG", fullName, OperateType.ALTER, 0L);
  }
}
