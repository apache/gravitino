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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestFunctionHookDispatcher {

  @Test
  public void testRegisterFunctionSetOwnerAfterRegister() throws Exception {
    NameIdentifier functionIdentifier =
        NameIdentifier.of("metalake1", "catalog1", "schema1", "func1");
    FunctionDefinition[] definitions = new FunctionDefinition[] {};
    FunctionDispatcher dispatcher = Mockito.mock(FunctionDispatcher.class);
    Function registeredFunction = Mockito.mock(Function.class);
    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);

    CatalogManager catalogManager = catalogManagerWith(Capability.DEFAULT);

    Mockito.when(
            dispatcher.registerFunction(
                Mockito.eq(functionIdentifier),
                Mockito.eq("comment"),
                Mockito.eq(FunctionType.SCALAR),
                Mockito.eq(true),
                Mockito.eq(definitions)))
        .thenReturn(registeredFunction);

    FunctionHookDispatcher hookDispatcher =
        new FunctionHookDispatcher(dispatcher, () -> ownerDispatcher, catalogManager);
    Function result =
        hookDispatcher.registerFunction(
            functionIdentifier, "comment", FunctionType.SCALAR, true, definitions);

    assertSame(registeredFunction, result);

    ArgumentCaptor<MetadataObject> metadataObjectCaptor =
        ArgumentCaptor.forClass(MetadataObject.class);
    Mockito.verify(ownerDispatcher)
        .setOwner(
            Mockito.eq("metalake1"),
            metadataObjectCaptor.capture(),
            Mockito.eq(AuthConstants.ANONYMOUS_USER),
            Mockito.eq(Owner.Type.USER));
    assertEquals(MetadataObject.Type.FUNCTION, metadataObjectCaptor.getValue().type());
    assertEquals("catalog1.schema1.func1", metadataObjectCaptor.getValue().fullName());
  }

  @Test
  public void testRegisterFunctionSucceedsWhenOwnerDispatcherIsDisabled() {
    NameIdentifier functionIdentifier =
        NameIdentifier.of("metalake1", "catalog1", "schema1", "func1");
    FunctionDefinition[] definitions = new FunctionDefinition[] {};
    FunctionDispatcher dispatcher = Mockito.mock(FunctionDispatcher.class);
    Function registeredFunction = Mockito.mock(Function.class);

    Mockito.when(
            dispatcher.registerFunction(
                Mockito.eq(functionIdentifier),
                Mockito.eq("comment"),
                Mockito.eq(FunctionType.SCALAR),
                Mockito.eq(true),
                Mockito.eq(definitions)))
        .thenReturn(registeredFunction);

    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    FunctionHookDispatcher hookDispatcher =
        new FunctionHookDispatcher(dispatcher, () -> null, catalogManager);
    Function result =
        hookDispatcher.registerFunction(
            functionIdentifier, "comment", FunctionType.SCALAR, true, definitions);

    assertSame(registeredFunction, result);
    Mockito.verify(dispatcher)
        .registerFunction(functionIdentifier, "comment", FunctionType.SCALAR, true, definitions);
    Mockito.verifyNoInteractions(catalogManager);
  }

  @Test
  public void testRegisterFunctionSetsOwnerWithNormalizedIdentifier() throws Exception {
    // Verifies the hook applies Capability.Scope.FUNCTION normalization before setOwner, so the
    // owner relation references the same identifier that NormalizeDispatcher persists under.
    CatalogManager catalogManager = catalogManagerWith(new CaseInsensitiveCapability());

    OwnerDispatcher mockOwnerDispatcher = Mockito.mock(OwnerDispatcher.class);
    FunctionDispatcher mockFunctionDispatcher = Mockito.mock(FunctionDispatcher.class);
    Function mockFunction = Mockito.mock(Function.class);
    FunctionDefinition[] definitions = new FunctionDefinition[] {};
    Mockito.when(
            mockFunctionDispatcher.registerFunction(
                any(), any(), any(), Mockito.anyBoolean(), any()))
        .thenReturn(mockFunction);

    FunctionHookDispatcher hook =
        new FunctionHookDispatcher(
            mockFunctionDispatcher, () -> mockOwnerDispatcher, catalogManager);
    NameIdentifier ident = NameIdentifier.of("metalake1", "catalog1", "SCHEMA_NORM", "MY_FUNC");
    hook.registerFunction(ident, "comment", FunctionType.SCALAR, true, definitions);

    ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
    Mockito.verify(mockOwnerDispatcher)
        .setOwner(eq("metalake1"), captor.capture(), any(), eq(Owner.Type.USER));
    assertEquals("my_func", captor.getValue().name());
    assertEquals("catalog1.schema_norm", captor.getValue().parent());
  }

  @Test
  public void testRegisterFunctionThrowsWhenSetOwnerFails() throws Exception {
    OwnerDispatcher mockOwnerDispatcher = Mockito.mock(OwnerDispatcher.class);
    Mockito.doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    CatalogManager catalogManager = catalogManagerWith(Capability.DEFAULT);

    FunctionDispatcher mockFunctionDispatcher = Mockito.mock(FunctionDispatcher.class);
    Function mockFunction = Mockito.mock(Function.class);
    FunctionDefinition[] definitions = new FunctionDefinition[] {};
    Mockito.when(
            mockFunctionDispatcher.registerFunction(
                any(), any(), any(), Mockito.anyBoolean(), any()))
        .thenReturn(mockFunction);

    FunctionHookDispatcher hook =
        new FunctionHookDispatcher(
            mockFunctionDispatcher, () -> mockOwnerDispatcher, catalogManager);
    NameIdentifier ident =
        NameIdentifier.of("metalake1", "catalog1", "schema_owner_fail", "func_owner_fail");
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> hook.registerFunction(ident, "comment", FunctionType.SCALAR, true, definitions));
    assertEquals("Set owner failed", thrown.getMessage());
  }

  @Test
  public void testDropFunctionRemovesPrivilegesWithNormalizedIdentifierAfterSuccessfulDrop()
      throws Exception {
    NameIdentifier functionIdentifier =
        NameIdentifier.of("metalake1", "catalog1", "SCHEMA1", "FUNC1");
    NameIdentifier normalizedIdentifier =
        NameIdentifier.of("metalake1", "catalog1", "schema1", "func1");
    FunctionDispatcher dispatcher = Mockito.mock(FunctionDispatcher.class);
    Mockito.when(dispatcher.dropFunction(functionIdentifier))
        .thenReturn(true, false)
        .thenThrow(new RuntimeException("Drop failed"));
    CatalogManager catalogManager = catalogManagerWith(new CaseInsensitiveCapability());

    try (MockedStatic<AuthorizationUtils> authorizationUtils =
        Mockito.mockStatic(AuthorizationUtils.class)) {
      FunctionHookDispatcher hookDispatcher =
          new FunctionHookDispatcher(dispatcher, () -> null, catalogManager);

      assertTrue(hookDispatcher.dropFunction(functionIdentifier));
      assertFalse(hookDispatcher.dropFunction(functionIdentifier));
      RuntimeException thrown =
          assertThrows(
              RuntimeException.class, () -> hookDispatcher.dropFunction(functionIdentifier));
      assertEquals("Drop failed", thrown.getMessage());
      authorizationUtils.verify(
          () ->
              AuthorizationUtils.authorizationPluginRemovePrivileges(
                  normalizedIdentifier, Entity.EntityType.FUNCTION, Collections.emptyList()),
          Mockito.times(1));
      Mockito.verify(catalogManager, Mockito.times(1)).loadCatalogAndWrap(any());
    }
  }

  private static CatalogManager catalogManagerWith(Capability capability) throws Exception {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    CatalogManager.CatalogWrapper catalogWrapper =
        Mockito.mock(CatalogManager.CatalogWrapper.class);
    Mockito.when(catalogWrapper.capabilities()).thenReturn(capability);
    Mockito.when(catalogManager.loadCatalogAndWrap(any())).thenReturn(catalogWrapper);
    return catalogManager;
  }

  private static class CaseInsensitiveCapability implements Capability {
    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("case-insensitive");
    }
  }
}
