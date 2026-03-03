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

import com.google.common.collect.ImmutableList;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.catalog.TableDispatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestTableHookDispatcher {

  @Test
  public void testDropTableShouldNotRemovePrivilegesWhenDropFails() {
    TableDispatcher dispatcher = Mockito.mock(TableDispatcher.class);
    TableHookDispatcher hookDispatcher = new TableHookDispatcher(dispatcher);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");

    Mockito.when(dispatcher.dropTable(ident)).thenReturn(false);

    try (MockedStatic<AuthorizationUtils> authz = Mockito.mockStatic(AuthorizationUtils.class)) {
      authz
          .when(() -> AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.TABLE))
          .thenReturn(ImmutableList.of("/test"));

      boolean dropped = hookDispatcher.dropTable(ident);

      Assertions.assertFalse(dropped);
      authz.verify(
          () ->
              AuthorizationUtils.authorizationPluginRemovePrivileges(
                  ident, Entity.EntityType.TABLE, ImmutableList.of("/test")),
          Mockito.never());
    }
  }

  @Test
  public void testDropTableShouldRemovePrivilegesWhenDropSucceeds() {
    TableDispatcher dispatcher = Mockito.mock(TableDispatcher.class);
    TableHookDispatcher hookDispatcher = new TableHookDispatcher(dispatcher);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");

    Mockito.when(dispatcher.dropTable(ident)).thenReturn(true);

    try (MockedStatic<AuthorizationUtils> authz = Mockito.mockStatic(AuthorizationUtils.class)) {
      authz
          .when(() -> AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.TABLE))
          .thenReturn(ImmutableList.of("/test"));

      boolean dropped = hookDispatcher.dropTable(ident);

      Assertions.assertTrue(dropped);
      authz.verify(
          () ->
              AuthorizationUtils.authorizationPluginRemovePrivileges(
                  ident, Entity.EntityType.TABLE, ImmutableList.of("/test")),
          Mockito.times(1));
    }
  }

  @Test
  public void testPurgeTableShouldNotRemovePrivilegesWhenPurgeFails() {
    TableDispatcher dispatcher = Mockito.mock(TableDispatcher.class);
    TableHookDispatcher hookDispatcher = new TableHookDispatcher(dispatcher);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");

    Mockito.when(dispatcher.purgeTable(ident)).thenReturn(false);

    try (MockedStatic<AuthorizationUtils> authz = Mockito.mockStatic(AuthorizationUtils.class)) {
      authz
          .when(() -> AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.TABLE))
          .thenReturn(ImmutableList.of("/test"));

      boolean purged = hookDispatcher.purgeTable(ident);

      Assertions.assertFalse(purged);
      authz.verify(
          () ->
              AuthorizationUtils.authorizationPluginRemovePrivileges(
                  ident, Entity.EntityType.TABLE, ImmutableList.of("/test")),
          Mockito.never());
    }
  }

  @Test
  public void testPurgeTableShouldRemovePrivilegesWhenPurgeSucceeds() {
    TableDispatcher dispatcher = Mockito.mock(TableDispatcher.class);
    TableHookDispatcher hookDispatcher = new TableHookDispatcher(dispatcher);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");

    Mockito.when(dispatcher.purgeTable(ident)).thenReturn(true);

    try (MockedStatic<AuthorizationUtils> authz = Mockito.mockStatic(AuthorizationUtils.class)) {
      authz
          .when(() -> AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.TABLE))
          .thenReturn(ImmutableList.of("/test"));

      boolean purged = hookDispatcher.purgeTable(ident);

      Assertions.assertTrue(purged);
      authz.verify(
          () ->
              AuthorizationUtils.authorizationPluginRemovePrivileges(
                  ident, Entity.EntityType.TABLE, ImmutableList.of("/test")),
          Mockito.times(1));
    }
  }
}
