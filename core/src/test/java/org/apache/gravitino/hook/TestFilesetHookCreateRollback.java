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
package org.apache.gravitino.hook;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.file.Fileset;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestFilesetHookCreateRollback {

  @Test
  public void testCreateFilesetThrowsPostHookExceptionWhenRollbackSucceeds() throws Exception {
    GravitinoEnv gravitinoEnv = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(gravitinoEnv, "ownerDispatcher", true);
    Object originalCatalogManager = FieldUtils.readField(gravitinoEnv, "catalogManager", true);

    FilesetDispatcher dispatcher = mock(FilesetDispatcher.class);
    Fileset fileset = mock(Fileset.class);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "fileset");
    RuntimeException postHookException = new RuntimeException("post-hook failed");

    OwnerDispatcher ownerDispatcher = mock(OwnerDispatcher.class);
    doThrow(postHookException)
        .when(ownerDispatcher)
        .setOwner(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any());
    when(dispatcher.createMultipleLocationFileset(
            eq(ident), any(), any(), anyMap(), anyMap(), anyMap(), anyMap()))
        .thenReturn(fileset);

    CatalogManager catalogManager = mock(CatalogManager.class);
    CatalogManager.CatalogWrapper catalogWrapper = mock(CatalogManager.CatalogWrapper.class);
    when(catalogManager.loadCatalogAndWrap(any())).thenReturn(catalogWrapper);
    when(catalogWrapper.capabilities()).thenReturn(Capability.DEFAULT);

    FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", ownerDispatcher, true);
    FieldUtils.writeField(gravitinoEnv, "catalogManager", catalogManager, true);

    try {
      FilesetHookDispatcher hookDispatcher = new FilesetHookDispatcher(dispatcher);
      RuntimeException thrown =
          assertThrowsExactly(
              RuntimeException.class,
              () ->
                  hookDispatcher.createMultipleLocationFileset(
                      ident,
                      "comment",
                      Fileset.Type.MANAGED,
                      Collections.emptyMap(),
                      Collections.emptyMap(),
                      Collections.emptyMap(),
                      Collections.emptyMap()));
      assertSame(postHookException, thrown);
      verify(dispatcher).dropFileset(ident);
    } finally {
      FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", originalOwnerDispatcher, true);
      FieldUtils.writeField(gravitinoEnv, "catalogManager", originalCatalogManager, true);
    }
  }

  @Test
  public void testCreateFilesetRollbackExceptionDoesNotMaskPostHookException() throws Exception {
    GravitinoEnv gravitinoEnv = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(gravitinoEnv, "ownerDispatcher", true);
    Object originalCatalogManager = FieldUtils.readField(gravitinoEnv, "catalogManager", true);

    FilesetDispatcher dispatcher = mock(FilesetDispatcher.class);
    Fileset fileset = mock(Fileset.class);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "fileset");
    RuntimeException postHookException = new RuntimeException("post-hook failed");
    RuntimeException rollbackException = new RuntimeException("rollback failed");

    OwnerDispatcher ownerDispatcher = mock(OwnerDispatcher.class);
    doThrow(postHookException)
        .when(ownerDispatcher)
        .setOwner(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any());
    when(dispatcher.createMultipleLocationFileset(
            eq(ident), any(), any(), anyMap(), anyMap(), anyMap(), anyMap()))
        .thenReturn(fileset);
    doThrow(rollbackException).when(dispatcher).dropFileset(ident);

    CatalogManager catalogManager = mock(CatalogManager.class);
    CatalogManager.CatalogWrapper catalogWrapper = mock(CatalogManager.CatalogWrapper.class);
    when(catalogManager.loadCatalogAndWrap(any())).thenReturn(catalogWrapper);
    when(catalogWrapper.capabilities()).thenReturn(Capability.DEFAULT);

    FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", ownerDispatcher, true);
    FieldUtils.writeField(gravitinoEnv, "catalogManager", catalogManager, true);

    try {
      FilesetHookDispatcher hookDispatcher = new FilesetHookDispatcher(dispatcher);
      RuntimeException thrown =
          assertThrowsExactly(
              RuntimeException.class,
              () ->
                  hookDispatcher.createMultipleLocationFileset(
                      ident,
                      "comment",
                      Fileset.Type.MANAGED,
                      Collections.emptyMap(),
                      Collections.emptyMap(),
                      Collections.emptyMap(),
                      Collections.emptyMap()));
      assertSame(postHookException, thrown);
      assertTrue(Arrays.stream(thrown.getSuppressed()).anyMatch(t -> t == rollbackException));
      verify(dispatcher).dropFileset(ident);
    } finally {
      FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", originalOwnerDispatcher, true);
      FieldUtils.writeField(gravitinoEnv, "catalogManager", originalCatalogManager, true);
    }
  }
}
