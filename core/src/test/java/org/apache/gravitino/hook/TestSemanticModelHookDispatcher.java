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

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestSemanticModelHookDispatcher {

  private static final NameIdentifier IDENT =
      NameIdentifier.of("metalake1", "catalog1", "schema1", "sales_model");

  @Test
  public void testCreateSemanticModelSetsCreatorAsOwner() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(env, "ownerDispatcher", true);
    Object originalCatalogManager = FieldUtils.readField(env, "catalogManager", true);

    SemanticModelDispatcher dispatcher = Mockito.mock(SemanticModelDispatcher.class);
    SemanticModelDefinition definition = Mockito.mock(SemanticModelDefinition.class);
    SemanticModel created = Mockito.mock(SemanticModel.class);
    Mockito.when(created.name()).thenReturn("sales_model");
    Mockito.when(dispatcher.createSemanticModel(eq(IDENT), eq("comment"), eq(definition), any()))
        .thenReturn(created);

    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);

    FieldUtils.writeField(env, "ownerDispatcher", ownerDispatcher, true);
    FieldUtils.writeField(env, "catalogManager", mockCatalogManager(Capability.DEFAULT), true);
    try {
      SemanticModelHookDispatcher hook = new SemanticModelHookDispatcher(dispatcher);
      SemanticModel result =
          hook.createSemanticModel(IDENT, "comment", definition, ImmutableMap.of());

      assertSame(created, result);

      ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
      Mockito.verify(ownerDispatcher)
          .setOwner(
              eq("metalake1"),
              captor.capture(),
              eq(AuthConstants.ANONYMOUS_USER),
              eq(Owner.Type.USER));
      assertEquals(MetadataObject.Type.SEMANTIC_MODEL, captor.getValue().type());
      assertEquals("catalog1.schema1.sales_model", captor.getValue().fullName());
    } finally {
      FieldUtils.writeField(env, "ownerDispatcher", originalOwnerDispatcher, true);
      FieldUtils.writeField(env, "catalogManager", originalCatalogManager, true);
    }
  }

  @Test
  public void testCreateSemanticModelSetsOwnerWithNormalizedIdentifier() throws Exception {
    // The NormalizeDispatcher case-folds the parent schema against the catalog capability while
    // keeping the Gravitino-owned model name, so the owner must be attached to that same
    // identifier.
    GravitinoEnv env = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(env, "ownerDispatcher", true);
    Object originalCatalogManager = FieldUtils.readField(env, "catalogManager", true);

    NameIdentifier ident = NameIdentifier.of("metalake1", "catalog1", "SCHEMA_NORM", "Sales_Model");
    SemanticModelDispatcher dispatcher = Mockito.mock(SemanticModelDispatcher.class);
    SemanticModelDefinition definition = Mockito.mock(SemanticModelDefinition.class);
    SemanticModel created = Mockito.mock(SemanticModel.class);
    Mockito.when(created.name()).thenReturn("Sales_Model");
    Mockito.when(dispatcher.createSemanticModel(any(), any(), any(), any())).thenReturn(created);

    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);

    FieldUtils.writeField(env, "ownerDispatcher", ownerDispatcher, true);
    FieldUtils.writeField(
        env, "catalogManager", mockCatalogManager(new CaseInsensitiveCapability()), true);
    try {
      SemanticModelHookDispatcher hook = new SemanticModelHookDispatcher(dispatcher);
      hook.createSemanticModel(ident, "comment", definition, ImmutableMap.of());

      ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
      Mockito.verify(ownerDispatcher)
          .setOwner(eq("metalake1"), captor.capture(), any(), eq(Owner.Type.USER));
      assertEquals(
          "catalog1.schema_norm",
          captor.getValue().parent(),
          "The schema component must be lowercased by the catalog capability");
      assertEquals(
          "Sales_Model",
          captor.getValue().name(),
          "The Semantic Model name is Gravitino-owned and must not be case-folded by the catalog");
    } finally {
      FieldUtils.writeField(env, "ownerDispatcher", originalOwnerDispatcher, true);
      FieldUtils.writeField(env, "catalogManager", originalCatalogManager, true);
    }
  }

  @Test
  public void testCreateSemanticModelSucceedsWhenOwnerDispatcherIsDisabled() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(env, "ownerDispatcher", true);

    SemanticModelDispatcher dispatcher = Mockito.mock(SemanticModelDispatcher.class);
    SemanticModelDefinition definition = Mockito.mock(SemanticModelDefinition.class);
    SemanticModel created = Mockito.mock(SemanticModel.class);
    Mockito.when(dispatcher.createSemanticModel(any(), any(), any(), any())).thenReturn(created);

    FieldUtils.writeField(env, "ownerDispatcher", null, true);
    try {
      SemanticModelHookDispatcher hook = new SemanticModelHookDispatcher(dispatcher);
      SemanticModel result =
          hook.createSemanticModel(IDENT, "comment", definition, ImmutableMap.of());

      assertSame(created, result);
      Mockito.verify(dispatcher)
          .createSemanticModel(IDENT, "comment", definition, ImmutableMap.of());
    } finally {
      FieldUtils.writeField(env, "ownerDispatcher", originalOwnerDispatcher, true);
    }
  }

  @Test
  public void testCreateSemanticModelThrowsWhenSetOwnerFails() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(env, "ownerDispatcher", true);
    Object originalCatalogManager = FieldUtils.readField(env, "catalogManager", true);

    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);
    Mockito.doThrow(new RuntimeException("Set owner failed"))
        .when(ownerDispatcher)
        .setOwner(any(), any(), any(), any());

    SemanticModelDispatcher dispatcher = Mockito.mock(SemanticModelDispatcher.class);
    SemanticModelDefinition definition = Mockito.mock(SemanticModelDefinition.class);
    SemanticModel created = Mockito.mock(SemanticModel.class);
    Mockito.when(created.name()).thenReturn("sales_model");
    Mockito.when(dispatcher.createSemanticModel(any(), any(), any(), any())).thenReturn(created);

    FieldUtils.writeField(env, "ownerDispatcher", ownerDispatcher, true);
    FieldUtils.writeField(env, "catalogManager", mockCatalogManager(Capability.DEFAULT), true);
    try {
      SemanticModelHookDispatcher hook = new SemanticModelHookDispatcher(dispatcher);
      RuntimeException thrown =
          assertThrows(
              RuntimeException.class,
              () -> hook.createSemanticModel(IDENT, "comment", definition, ImmutableMap.of()));
      assertEquals("Set owner failed", thrown.getMessage());
    } finally {
      FieldUtils.writeField(env, "ownerDispatcher", originalOwnerDispatcher, true);
      FieldUtils.writeField(env, "catalogManager", originalCatalogManager, true);
    }
  }

  @Test
  public void testNonCreateOperationsDelegateWithoutOwnerChanges() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(env, "ownerDispatcher", true);

    SemanticModelDispatcher dispatcher = Mockito.mock(SemanticModelDispatcher.class);
    SemanticModel loaded = Mockito.mock(SemanticModel.class);
    SemanticModel altered = Mockito.mock(SemanticModel.class);
    NameIdentifier[] listed = new NameIdentifier[] {IDENT};
    SemanticModelChange change = SemanticModelChange.rename("renamed_model");

    Namespace namespace = Namespace.of("metalake1", "catalog1", "schema1");
    Mockito.when(dispatcher.listSemanticModels(namespace)).thenReturn(listed);
    Mockito.when(dispatcher.loadSemanticModel(IDENT)).thenReturn(loaded);
    Mockito.when(dispatcher.alterSemanticModel(IDENT, change)).thenReturn(altered);
    Mockito.when(dispatcher.dropSemanticModel(IDENT)).thenReturn(true);
    Mockito.when(dispatcher.semanticModelExists(IDENT)).thenReturn(true);

    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);
    FieldUtils.writeField(env, "ownerDispatcher", ownerDispatcher, true);
    try {
      SemanticModelHookDispatcher hook = new SemanticModelHookDispatcher(dispatcher);

      assertSame(listed, hook.listSemanticModels(namespace));
      assertSame(loaded, hook.loadSemanticModel(IDENT));
      assertSame(altered, hook.alterSemanticModel(IDENT, change));
      assertTrue(hook.dropSemanticModel(IDENT));
      assertTrue(hook.semanticModelExists(IDENT));

      Mockito.verifyNoInteractions(ownerDispatcher);
    } finally {
      FieldUtils.writeField(env, "ownerDispatcher", originalOwnerDispatcher, true);
    }
  }

  private static CatalogManager mockCatalogManager(Capability capability) throws Exception {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    CatalogManager.CatalogWrapper wrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    Mockito.when(wrapper.capabilities()).thenReturn(capability);
    Mockito.when(catalogManager.loadCatalogAndWrap(any())).thenReturn(wrapper);
    return catalogManager;
  }

  private static class CaseInsensitiveCapability implements Capability {
    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("case-insensitive");
    }
  }
}
