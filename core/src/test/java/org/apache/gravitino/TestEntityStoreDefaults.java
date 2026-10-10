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
package org.apache.gravitino;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;
import org.apache.gravitino.meta.UserEntity;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Verifies which overload each default {@link EntityStore} method delegates to. */
public class TestEntityStoreDefaults {

  private final EntityStore store = mock(EntityStore.class, Mockito.CALLS_REAL_METHODS);

  @Test
  public void testListWithoutAllFieldsSkipsHighCostFields() throws IOException {
    Namespace namespace = Namespace.of("metalake", "user");
    Mockito.doReturn(Collections.emptyList())
        .when(store)
        .list(namespace, UserEntity.class, Entity.EntityType.USER, false);

    store.list(namespace, UserEntity.class, Entity.EntityType.USER);

    verify(store).list(namespace, UserEntity.class, Entity.EntityType.USER, false);
  }

  @Test
  public void testPutWithoutFlagDoesNotOverwrite() throws IOException {
    UserEntity user = mock(UserEntity.class);
    Mockito.doNothing().when(store).put(user, false);

    store.put(user);

    verify(store).put(user, false);
  }

  @Test
  public void testDeleteWithoutFlagDoesNotCascade() throws IOException {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog");
    Mockito.doReturn(true).when(store).delete(ident, Entity.EntityType.CATALOG, false);

    store.delete(ident, Entity.EntityType.CATALOG);

    verify(store).delete(ident, Entity.EntityType.CATALOG, false);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testExecuteInTransactionIsUnsupportedByDefault() {
    assertThrows(UnsupportedOperationException.class, () -> store.executeInTransaction(() -> null));
  }
}
