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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.catalog.ViewNormalizeDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests for {@link ViewHookDispatcher}. */
public class TestViewHookDispatcher {

  private static final String METALAKE = "metalake";
  private static final String CATALOG = "catalog";

  @Test
  public void testCreateViewSetsOwnerWithNormalizedIdentifier() throws Exception {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.capability()).thenReturn(new CaseInsensitiveCapability());
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);

    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);
    ViewDispatcher dispatcher = Mockito.mock(ViewDispatcher.class);
    View createdView = Mockito.mock(View.class);
    Mockito.when(dispatcher.createView(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(createdView);
    ViewDispatcher hook =
        new ViewNormalizeDispatcher(
            new ViewHookDispatcher(dispatcher, () -> ownerDispatcher), catalogManager);
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "SCHEMA_NORM", "MY_VIEW");

    try (MockedStatic<PrincipalUtils> principalUtils = Mockito.mockStatic(PrincipalUtils.class)) {
      principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("creator");
      assertSame(createdView, createView(hook, ident));
    }

    ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
    Mockito.verify(ownerDispatcher)
        .setOwner(eq(METALAKE), captor.capture(), eq("creator"), eq(Owner.Type.USER));
    assertEquals(MetadataObject.Type.VIEW, captor.getValue().type());
    assertEquals("my_view", captor.getValue().name());
    assertEquals(CATALOG + ".schema_norm", captor.getValue().parent());
  }

  @Test
  public void testCreateViewSkipsOwnerWhenAuthorizationDisabled() {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    ViewDispatcher dispatcher = Mockito.mock(ViewDispatcher.class);
    View createdView = Mockito.mock(View.class);
    Mockito.when(dispatcher.createView(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(createdView);
    ViewHookDispatcher hook = new ViewHookDispatcher(dispatcher, () -> null);

    assertSame(
        createdView, createView(hook, NameIdentifier.of(METALAKE, CATALOG, "schema", "view")));

    Mockito.verifyNoInteractions(catalogManager);
  }

  @Test
  public void testCreateViewPropagatesOwnerFailure() throws Exception {
    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);
    Mockito.doThrow(new RuntimeException("Set owner failed"))
        .when(ownerDispatcher)
        .setOwner(any(), any(), any(), any());
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.capability()).thenReturn(Capability.DEFAULT);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    ViewDispatcher dispatcher = Mockito.mock(ViewDispatcher.class);
    Mockito.when(dispatcher.createView(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Mockito.mock(View.class));
    ViewHookDispatcher hook = new ViewHookDispatcher(dispatcher, () -> ownerDispatcher);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                createView(hook, NameIdentifier.of(METALAKE, CATALOG, "schema", "owner_failure")));

    assertEquals("Set owner failed", thrown.getMessage());
  }

  @Test
  public void testRenameViewUpdatesAuthorizationMapping() {
    ViewDispatcher dispatcher = Mockito.mock(ViewDispatcher.class);
    ViewHookDispatcher hook = new ViewHookDispatcher(dispatcher, () -> null);
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "schema", "view");
    View alteredView = Mockito.mock(View.class);
    ViewChange setChange = ViewChange.setProperty("key", "value");
    ViewChange renameChange = ViewChange.rename("newName");
    Mockito.when(dispatcher.alterView(ident, setChange)).thenReturn(alteredView);
    Mockito.when(dispatcher.alterView(ident, renameChange)).thenReturn(alteredView);

    try (MockedStatic<AuthorizationUtils> authorizationUtils =
        Mockito.mockStatic(AuthorizationUtils.class)) {
      assertSame(alteredView, hook.alterView(ident, setChange));
      authorizationUtils.verifyNoInteractions();

      assertSame(alteredView, hook.alterView(ident, renameChange));
      authorizationUtils.verify(
          () ->
              AuthorizationUtils.authorizationPluginRenamePrivileges(
                  ident, Entity.EntityType.VIEW, "newName"));
    }
  }

  private View createView(ViewDispatcher hook, NameIdentifier ident) {
    Representation[] representations =
        new Representation[] {
          SQLRepresentation.builder().withDialect("trino").withSql("SELECT 1").build()
        };
    return hook.createView(
        ident, "comment", new Column[0], representations, null, null, ImmutableMap.of());
  }

  private static class CaseInsensitiveCapability implements Capability {
    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("case-insensitive");
    }
  }
}
