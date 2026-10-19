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
package org.apache.gravitino.lance.service.authorization;

import java.io.IOException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.PermissionDeniedException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class TestLanceExistingTableReadGuard {
  private static final NameIdentifier IDENTIFIER =
      NameIdentifier.of("metalake", "catalog", "schema", "table");
  private Object savedStore;
  private EntityStore store;
  private MockedStatic<MetadataAuthzHelper> access;
  private LanceAuthorizationMetadataFilter filter;

  @BeforeEach
  void setUp() throws Exception {
    savedStore = FieldUtils.readField(GravitinoEnv.getInstance(), "entityStore", true);
    store = Mockito.mock(EntityStore.class);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", store, true);
    access = Mockito.mockStatic(MetadataAuthzHelper.class);
    filter = new LanceAuthorizationMetadataFilter("metalake");
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      access.close();
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", savedStore, true);
    }
  }

  @Test
  void testMissingMetadataDoesNotRequireReadPermission() throws Exception {
    Mockito.when(store.exists(IDENTIFIER, Entity.EntityType.TABLE)).thenReturn(false);

    NoSuchTableException failure =
        Assertions.assertThrows(
            NoSuchTableException.class, () -> filter.checkReadTable("catalog", "schema", "table"));

    Assertions.assertTrue(failure.getMessage().contains(IDENTIFIER.toString()));
    access.verifyNoInteractions();
    verifyOnlyMetadataRead();
  }

  @Test
  void testExistingTableIsDeniedBeforeProviderAccess() throws Exception {
    Mockito.when(store.exists(IDENTIFIER, Entity.EntityType.TABLE)).thenReturn(true);

    PermissionDeniedException failure =
        Assertions.assertThrows(
            PermissionDeniedException.class,
            () -> filter.checkReadTable("catalog", "schema", "table"));

    Assertions.assertTrue(failure.getMessage().contains(IDENTIFIER.toString()));
    verifyOnlyMetadataRead();
  }

  @Test
  void testExistingTableRequiresFullReadExpression() throws Exception {
    Mockito.when(store.exists(IDENTIFIER, Entity.EntityType.TABLE)).thenReturn(true);
    access
        .when(
            () ->
                MetadataAuthzHelper.checkAccess(
                    IDENTIFIER,
                    Entity.EntityType.TABLE,
                    AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION))
        .thenReturn(true);

    Assertions.assertDoesNotThrow(() -> filter.checkReadTable("catalog", "schema", "table"));

    access.verify(
        () ->
            MetadataAuthzHelper.checkAccess(
                IDENTIFIER,
                Entity.EntityType.TABLE,
                AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION));
    verifyOnlyMetadataRead();
  }

  @Test
  void testMetadataFailurePreservesCauseAndDoesNotAuthorize() throws Exception {
    IOException unavailable = new IOException("metadata store unavailable");
    Mockito.when(store.exists(IDENTIFIER, Entity.EntityType.TABLE)).thenThrow(unavailable);

    RuntimeException failure =
        Assertions.assertThrows(
            RuntimeException.class, () -> filter.checkReadTable("catalog", "schema", "table"));

    Assertions.assertSame(unavailable, failure.getCause());
    Assertions.assertTrue(failure.getMessage().contains(IDENTIFIER.toString()));
    access.verifyNoInteractions();
    verifyOnlyMetadataRead();
  }

  private void verifyOnlyMetadataRead() throws Exception {
    Mockito.verify(store).exists(IDENTIFIER, Entity.EntityType.TABLE);
    Mockito.verifyNoMoreInteractions(store);
  }
}
