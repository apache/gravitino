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
import java.util.Map;
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
  public void testCreatePostHookRollback() throws Exception {
    RuntimeException postHook = new RuntimeException("post-hook failed");
    runCreateWithPostHookFailure(
        postHook,
        null,
        (dispatcher, ident, thrown) -> {
          assertSame(postHook, thrown);
          Mockito.verify(dispatcher).dropCatalog(ident, true);
        });
  }

  @Test
  public void testCreateRollbackSuppressed() throws Exception {
    RuntimeException postHook = new RuntimeException("post-hook failed");
    RuntimeException rollback = new RuntimeException("rollback failed");
    runCreateWithPostHookFailure(
        postHook,
        rollback,
        (dispatcher, ident, thrown) -> {
          assertSame(postHook, thrown);
          assertTrue(Arrays.stream(thrown.getSuppressed()).anyMatch(t -> t == rollback));
          Mockito.verify(dispatcher).dropCatalog(ident, true);
        });
  }

  @FunctionalInterface
  private interface PostHookAssert {
    void check(CatalogDispatcher dispatcher, NameIdentifier ident, RuntimeException thrown);
  }

  private static void runCreateWithPostHookFailure(
      RuntimeException postHook, RuntimeException dropFailure, PostHookAssert asserts)
      throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    Object savedOwner = FieldUtils.readField(env, "ownerDispatcher", true);
    Object savedFutureGrant = FieldUtils.readField(env, "futureGrantManager", true);

    CatalogDispatcher dispatcher = Mockito.mock(CatalogDispatcher.class);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog");
    OwnerDispatcher owner = Mockito.mock(OwnerDispatcher.class);
    Mockito.doThrow(postHook)
        .when(owner)
        .setOwner(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any());
    Mockito.when(
            dispatcher.createCatalog(
                Mockito.eq(ident),
                Mockito.eq(Catalog.Type.RELATIONAL),
                Mockito.eq("provider"),
                Mockito.eq("comment"),
                Mockito.anyMap(),
                Mockito.anyMap(),
                Mockito.anyMap()))
        .thenReturn(Mockito.mock(Catalog.class));
    if (dropFailure != null) {
      Mockito.doThrow(dropFailure).when(dispatcher).dropCatalog(ident, true);
    }

    FieldUtils.writeField(env, "ownerDispatcher", owner, true);
    FieldUtils.writeField(env, "futureGrantManager", null, true);
    try {
      CatalogHookDispatcher hook = new CatalogHookDispatcher(dispatcher);
      RuntimeException thrown =
          assertThrowsExactly(
              RuntimeException.class,
              () ->
                  hook.createCatalog(
                      ident, Catalog.Type.RELATIONAL, "provider", "comment", Map.of()));
      asserts.check(dispatcher, ident, thrown);
    } finally {
      FieldUtils.writeField(env, "ownerDispatcher", savedOwner, true);
      FieldUtils.writeField(env, "futureGrantManager", savedFutureGrant, true);
    }
  }
}
