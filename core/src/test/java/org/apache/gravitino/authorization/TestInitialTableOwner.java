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
package org.apache.gravitino.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.storage.relational.RelationalBackend;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests post-create owner dispatch, cache invalidation and fail-closed compatibility. */
public class TestInitialTableOwner {
  private static final String METALAKE = "metalake";
  private static final MetadataObject TABLE =
      MetadataObjects.of("catalog.schema", "table", MetadataObject.Type.TABLE);
  private static final NameIdentifier TABLE_IDENT =
      NameIdentifier.of(METALAKE, "catalog", "schema", "table");
  private static final NameIdentifier OWNER_IDENT = AuthorizationUtils.ofUser(METALAKE, "alice");

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testOnlyAssignedOwnerInvalidatesCachesAndNotifies(boolean assigned) throws Exception {
    RelationalBackend backend = Mockito.mock(RelationalBackend.class);
    EntityCache cache = Mockito.mock(EntityCache.class);
    RelationalEntityStore store = store(backend, cache);
    Mockito.when(
            backend.setInitialTableOwner(TABLE_IDENT, 42L, OWNER_IDENT, Entity.EntityType.USER))
        .thenReturn(assigned);
    OwnerManager manager = new OwnerManager(store);
    Mockito.when(backend.supportsInitialTableOwner()).thenReturn(assigned);
    Assertions.assertEquals(assigned, manager.supportsInitialTableOwner());
    AuthorizationPlugin plugin = Mockito.mock(AuthorizationPlugin.class);
    GravitinoEnv env = Mockito.mock(GravitinoEnv.class);
    try (MockedStatic<AuthorizationUtils> auth = Mockito.mockStatic(AuthorizationUtils.class);
        MockedStatic<GravitinoEnv> environment = Mockito.mockStatic(GravitinoEnv.class)) {
      environment.when(GravitinoEnv::getInstance).thenReturn(env);
      auth.when(() -> AuthorizationUtils.ofUser(METALAKE, "alice")).thenReturn(OWNER_IDENT);
      auth.when(
              () ->
                  AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                      eq(METALAKE), eq(TABLE), any()))
          .thenAnswer(
              invocation -> {
                Consumer<AuthorizationPlugin> consumer = invocation.getArgument(2);
                consumer.accept(plugin);
                return null;
              });
      Assertions.assertEquals(
          assigned, manager.setInitialTableOwner(METALAKE, TABLE, 42L, "alice", Owner.Type.USER));
      if (assigned) {
        Mockito.verify(cache).invalidate(TABLE_IDENT, Entity.EntityType.TABLE);
        Mockito.verify(cache).invalidate(OWNER_IDENT, Entity.EntityType.USER);
        Mockito.verify(plugin)
            .onOwnerSet(
                eq(TABLE),
                Mockito.isNull(),
                Mockito.argThat(
                    owner -> owner.name().equals("alice") && owner.type() == Owner.Type.USER));
      } else {
        Mockito.verifyNoInteractions(cache, plugin, env);
      }
    }
  }

  @Test
  void testStorageAndPrincipalFailuresPropagate() throws Exception {
    RelationalBackend backend = Mockito.mock(RelationalBackend.class);
    EntityCache cache = Mockito.mock(EntityCache.class);
    OwnerManager manager = new OwnerManager(store(backend, cache));
    IOException unavailable = new IOException("storage unavailable");
    Mockito.when(
            backend.setInitialTableOwner(TABLE_IDENT, 42L, OWNER_IDENT, Entity.EntityType.USER))
        .thenThrow(unavailable);
    RuntimeException failure =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> manager.setInitialTableOwner(METALAKE, TABLE, 42L, "alice", Owner.Type.USER));
    Assertions.assertSame(unavailable, failure.getCause());
    NoSuchEntityException missing = new NoSuchEntityException("user missing");
    Mockito.doThrow(missing)
        .when(backend)
        .setInitialTableOwner(TABLE_IDENT, 42L, OWNER_IDENT, Entity.EntityType.USER);
    NotFoundException principalFailure =
        Assertions.assertThrows(
            NotFoundException.class,
            () -> manager.setInitialTableOwner(METALAKE, TABLE, 42L, "alice", Owner.Type.USER));
    Assertions.assertSame(missing, principalFailure.getCause());
    Mockito.verifyNoInteractions(cache);
  }

  @Test
  void testUnsupportedImplementationsDoNotFallBackToUnfencedAssignment() throws Exception {
    OwnerDispatcher owners = Mockito.mock(OwnerDispatcher.class, Mockito.CALLS_REAL_METHODS);
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> owners.setInitialTableOwner(METALAKE, TABLE, 42L, "alice", Owner.Type.USER));
    Mockito.verify(owners, Mockito.never()).setOwner(any(), any(), any(), any());
    SupportsRelationOperations relations =
        Mockito.mock(SupportsRelationOperations.class, Mockito.CALLS_REAL_METHODS);
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            relations.setInitialTableOwner(TABLE_IDENT, 42L, OWNER_IDENT, Entity.EntityType.USER));
    Mockito.verify(relations, Mockito.never())
        .insertRelation(any(), any(), any(), any(), any(), Mockito.anyBoolean());
  }

  @Test
  void testGroupPrincipalAndInvalidInputs() throws Exception {
    RelationalBackend backend = Mockito.mock(RelationalBackend.class);
    OwnerManager manager = new OwnerManager(store(backend, Mockito.mock(EntityCache.class)));
    NameIdentifier group = AuthorizationUtils.ofGroup(METALAKE, "group");
    Assertions.assertFalse(
        manager.setInitialTableOwner(METALAKE, TABLE, 42L, "group", Owner.Type.GROUP));
    Mockito.verify(backend).setInitialTableOwner(TABLE_IDENT, 42L, group, Entity.EntityType.GROUP);
    Mockito.clearInvocations(backend);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            manager.setInitialTableOwner(
                METALAKE,
                MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA),
                42L,
                "alice",
                Owner.Type.USER));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> manager.setInitialTableOwner(METALAKE, TABLE, 42L, "alice", null));
    Mockito.verifyNoInteractions(backend);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            OwnerMetaService.getInstance()
                .setInitialTableOwner(TABLE_IDENT, 42L, OWNER_IDENT, Entity.EntityType.TABLE));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            OwnerMetaService.getInstance()
                .setInitialTableOwner(
                    TABLE_IDENT,
                    42L,
                    AuthorizationUtils.ofUser("other", "alice"),
                    Entity.EntityType.USER));
  }

  private RelationalEntityStore store(RelationalBackend backend, EntityCache cache)
      throws Exception {
    RelationalEntityStore store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", cache, true);
    return store;
  }
}
