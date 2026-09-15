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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.TableNormalizeDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestTableHookDispatcher {

  private static final String METALAKE = "metalake";
  private static final String CATALOG = "catalog";

  @Test
  public void testDropAuthorizationPrivilege() {
    TableDispatcher dispatcher = Mockito.mock(TableDispatcher.class);
    TableHookDispatcher hook = new TableHookDispatcher(dispatcher, () -> null);
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "schema", "table");
    List<String> locations = ImmutableList.of("/test");
    Mockito.when(dispatcher.dropTable(ident)).thenReturn(true);

    try (MockedStatic<AuthorizationUtils> authorizationUtils =
        Mockito.mockStatic(AuthorizationUtils.class)) {
      authorizationUtils
          .when(() -> AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.TABLE))
          .thenReturn(locations);

      assertTrue(hook.dropTable(ident));

      authorizationUtils.verify(
          () ->
              AuthorizationUtils.authorizationPluginRemovePrivileges(
                  ident, Entity.EntityType.TABLE, locations));
    }
  }

  @Test
  public void testCreateTableSetsOwnerWithNormalizedIdentifier() throws Exception {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.capability()).thenReturn(new CaseInsensitiveCapability());
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);

    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);
    TableDispatcher dispatcher = Mockito.mock(TableDispatcher.class);
    Table createdTable = Mockito.mock(Table.class);
    Mockito.when(dispatcher.createTable(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(createdTable);
    TableDispatcher hook =
        new TableNormalizeDispatcher(
            new TableHookDispatcher(dispatcher, () -> ownerDispatcher), catalogManager);
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "SCHEMA_NORM", "MY_TABLE");

    assertSame(
        createdTable,
        hook.createTable(
            ident,
            new Column[0],
            "comment",
            ImmutableMap.of(),
            new Transform[0],
            Distributions.NONE,
            new SortOrder[0],
            new Index[0]));

    ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
    Mockito.verify(ownerDispatcher)
        .setOwner(eq(METALAKE), captor.capture(), any(), eq(Owner.Type.USER));
    assertEquals("my_table", captor.getValue().name());
    assertEquals(CATALOG + ".schema_norm", captor.getValue().parent());
  }

  @Test
  public void testCreateTableSkipsOwnerWhenAuthorizationDisabled() {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    TableDispatcher dispatcher = Mockito.mock(TableDispatcher.class);
    Table createdTable = Mockito.mock(Table.class);
    Mockito.when(dispatcher.createTable(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(createdTable);
    TableHookDispatcher hook = new TableHookDispatcher(dispatcher, () -> null);

    assertSame(
        createdTable,
        hook.createTable(
            NameIdentifier.of(METALAKE, CATALOG, "schema", "table"),
            new Column[0],
            "comment",
            ImmutableMap.of(),
            new Transform[0],
            Distributions.NONE,
            new SortOrder[0],
            new Index[0]));

    Mockito.verifyNoInteractions(catalogManager);
  }

  @Test
  public void testCreateTableThrowsWhenSetOwnerFails() throws Exception {
    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);
    Mockito.doThrow(new RuntimeException("Set owner failed"))
        .when(ownerDispatcher)
        .setOwner(any(), any(), any(), any());
    TableDispatcher dispatcher = Mockito.mock(TableDispatcher.class);
    Mockito.when(dispatcher.createTable(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Mockito.mock(Table.class));
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.capability()).thenReturn(Capability.DEFAULT);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    TableHookDispatcher hook = new TableHookDispatcher(dispatcher, () -> ownerDispatcher);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                hook.createTable(
                    NameIdentifier.of(METALAKE, CATALOG, "schema", "table"),
                    new Column[0],
                    "comment",
                    ImmutableMap.of(),
                    new Transform[0],
                    Distributions.NONE,
                    new SortOrder[0],
                    new Index[0]));

    assertEquals("Set owner failed", thrown.getMessage());
  }

  @Test
  public void testRenameAuthorizationPrivilege() {
    TableDispatcher dispatcher = Mockito.mock(TableDispatcher.class);
    TableHookDispatcher hook = new TableHookDispatcher(dispatcher, () -> null);
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "schema", "table");
    Table alteredTable = Mockito.mock(Table.class);
    TableChange setChange = TableChange.setProperty("key", "value");
    TableChange renameChange = TableChange.rename("newName");
    List<String> locations = ImmutableList.of("/test");
    Mockito.when(dispatcher.alterTable(ident, setChange)).thenReturn(alteredTable);
    Mockito.when(dispatcher.alterTable(ident, renameChange)).thenReturn(alteredTable);

    try (MockedStatic<AuthorizationUtils> authorizationUtils =
        Mockito.mockStatic(AuthorizationUtils.class)) {
      assertSame(alteredTable, hook.alterTable(ident, setChange));
      authorizationUtils.verifyNoInteractions();

      authorizationUtils
          .when(() -> AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.TABLE))
          .thenReturn(locations);
      assertSame(alteredTable, hook.alterTable(ident, renameChange));
      authorizationUtils.verify(
          () ->
              AuthorizationUtils.authorizationPluginRenamePrivileges(
                  ident, Entity.EntityType.TABLE, "newName", locations));
    }
  }

  private static class CaseInsensitiveCapability implements Capability {
    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("case-insensitive");
    }
  }
}
