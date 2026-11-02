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

import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager.CatalogWrapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Regression probe for catalog mutation markers published after the change-log commit. */
public class TestCatalogMutationMarkerRace {
  @Test
  void testLocalRemoteAndRepeatedEventsAlwaysInvalidateReloadedCatalog() {
    CatalogManager manager = new CatalogManager(new Config() {}, null, null, null);
    CatalogChangeLogListener listener = new CatalogChangeLogListener(manager);
    NameIdentifier ident = NameIdentifier.of("lake", "catalog");
    CatalogWrapper versionOne = Mockito.mock(CatalogWrapper.class);
    CatalogWrapper versionTwo = Mockito.mock(CatalogWrapper.class);
    try {
      manager.getCatalogCache().put(ident, versionOne);
      // Consumption may run before the local mutation caller has returned.
      listener.onEntityChange(
          List.of(
              new EntityChangeRecord(
                  1L, "lake", "CATALOG", ident.toString(), OperateType.ALTER, 0L)));
      Assertions.assertNull(manager.getCatalogCache().getIfPresent(ident));
      // The writer reloads its new catalog after its event was consumed.
      manager.getCatalogCache().put(ident, versionTwo);
      // A different node now changes the same catalog. This is not a local mutation.
      listener.onEntityChange(
          List.of(
              new EntityChangeRecord(
                  2L, "lake", "CATALOG", ident.toString(), OperateType.ALTER, 0L)));
      Assertions.assertNull(
          manager.getCatalogCache().getIfPresent(ident),
          "A previously consumed local event must not mask a future remote event");
      manager.getCatalogCache().put(ident, versionTwo);
      listener.onEntityChange(
          List.of(
              new EntityChangeRecord(
                  1L, "lake", "CATALOG", ident.toString(), OperateType.ALTER, 0L)));
      Assertions.assertNull(
          manager.getCatalogCache().getIfPresent(ident),
          "Replayed events must conservatively invalidate a reloaded catalog too");
    } finally {
      manager.close();
    }
  }
}
