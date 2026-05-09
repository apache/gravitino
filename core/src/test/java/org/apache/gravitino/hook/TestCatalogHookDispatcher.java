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

import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCatalogHookDispatcher {

  @Test
  public void testCreateCatalogThrowsPostHookExceptionWhenRollbackSucceeds() throws Exception {
    GravitinoEnv gravitinoEnv = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(gravitinoEnv, "ownerDispatcher", true);
    Object originalFutureGrantManager =
        FieldUtils.readField(gravitinoEnv, "futureGrantManager", true);

    CatalogDispatcher dispatcher = Mockito.mock(CatalogDispatcher.class);
    Catalog catalog = Mockito.mock(Catalog.class);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog");
    RuntimeException postHookException = new RuntimeException("post-hook failed");

    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);
    Mockito.doThrow(postHookException)
        .when(ownerDispatcher)
        .setOwner(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any());
    Mockito.when(
            dispatcher.createCatalog(
                Mockito.eq(ident),
                Mockito.eq(Catalog.Type.RELATIONAL),
                Mockito.eq("provider"),
                Mockito.eq("comment"),
                Mockito.anyMap()))
        .thenReturn(catalog);

    FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", ownerDispatcher, true);
    FieldUtils.writeField(gravitinoEnv, "futureGrantManager", null, true);

    try {
      CatalogHookDispatcher hookDispatcher = new CatalogHookDispatcher(dispatcher);
      RuntimeException thrown =
          assertThrowsExactly(
              RuntimeException.class,
              () ->
                  hookDispatcher.createCatalog(
                      ident,
                      Catalog.Type.RELATIONAL,
                      "provider",
                      "comment",
                      Collections.emptyMap()));
      assertSame(postHookException, thrown);

      Mockito.verify(dispatcher).dropCatalog(ident, true);
    } finally {
      FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", originalOwnerDispatcher, true);
      FieldUtils.writeField(gravitinoEnv, "futureGrantManager", originalFutureGrantManager, true);
    }
  }

  @Test
  public void testCreateCatalogRollbackExceptionDoesNotMaskPostHookException() throws Exception {
    GravitinoEnv gravitinoEnv = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(gravitinoEnv, "ownerDispatcher", true);
    Object originalFutureGrantManager =
        FieldUtils.readField(gravitinoEnv, "futureGrantManager", true);

    CatalogDispatcher dispatcher = Mockito.mock(CatalogDispatcher.class);
    Catalog catalog = Mockito.mock(Catalog.class);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog");
    RuntimeException postHookException = new RuntimeException("post-hook failed");
    RuntimeException rollbackException = new RuntimeException("rollback failed");

    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);
    Mockito.doThrow(postHookException)
        .when(ownerDispatcher)
        .setOwner(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any());
    Mockito.when(
            dispatcher.createCatalog(
                Mockito.eq(ident),
                Mockito.eq(Catalog.Type.RELATIONAL),
                Mockito.eq("provider"),
                Mockito.eq("comment"),
                Mockito.anyMap()))
        .thenReturn(catalog);
    Mockito.doThrow(rollbackException).when(dispatcher).dropCatalog(ident, true);

    FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", ownerDispatcher, true);
    FieldUtils.writeField(gravitinoEnv, "futureGrantManager", null, true);

    try {
      CatalogHookDispatcher hookDispatcher = new CatalogHookDispatcher(dispatcher);
      RuntimeException thrown =
          assertThrowsExactly(
              RuntimeException.class,
              () ->
                  hookDispatcher.createCatalog(
                      ident,
                      Catalog.Type.RELATIONAL,
                      "provider",
                      "comment",
                      Collections.emptyMap()));
      assertSame(postHookException, thrown);
      assertTrue(Arrays.stream(thrown.getSuppressed()).anyMatch(t -> t == rollbackException));

      Mockito.verify(dispatcher).dropCatalog(ident, true);
    } finally {
      FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", originalOwnerDispatcher, true);
      FieldUtils.writeField(gravitinoEnv, "futureGrantManager", originalFutureGrantManager, true);
    }
  }
}
