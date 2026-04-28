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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.model.Model;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestModelHookDispatcher {

  private ModelHookDispatcher hookDispatcher;
  private ModelDispatcher mockDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;
  private CatalogManager mockCatalogManager;
  private CatalogManager.CatalogWrapper mockCatalogWrapper;
  // Save the originals before each test and restore them in tearDown so we do not leak null
  // state into the GravitinoEnv singleton across tests.
  private OwnerDispatcher savedOwnerDispatcher;
  private CatalogManager savedCatalogManager;

  @BeforeEach
  public void setUp() throws Exception {
    mockDispatcher = mock(ModelDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
    mockCatalogManager = mock(CatalogManager.class);
    mockCatalogWrapper = mock(CatalogManager.CatalogWrapper.class);
    when(mockCatalogManager.loadCatalogAndWrap(any())).thenReturn(mockCatalogWrapper);
    when(mockCatalogWrapper.capabilities()).thenReturn(Capability.DEFAULT);
    savedOwnerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
    // Read the catalogManager field directly via reflection because the public accessor
    // Preconditions-checks for non-null, which would fail when GravitinoEnv has not been
    // initialized for this test class.
    savedCatalogManager =
        (CatalogManager) FieldUtils.readField(GravitinoEnv.getInstance(), "catalogManager", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", mockCatalogManager, true);
    hookDispatcher = new ModelHookDispatcher(mockDispatcher);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "ownerDispatcher", savedOwnerDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", savedCatalogManager, true);
  }

  @Test
  public void testRegisterModelSucceedsEvenIfSetOwnerFails() {
    NameIdentifier ident =
        NameIdentifier.of("test_metalake", "test_catalog", "test_schema", "test_model");
    Model mockModel = mock(Model.class);
    when(mockDispatcher.registerModel(any(NameIdentifier.class), any(String.class), any()))
        .thenReturn(mockModel);

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    Model result = hookDispatcher.registerModel(ident, "comment", Collections.emptyMap());

    Assertions.assertEquals(mockModel, result);
    verify(mockDispatcher).registerModel(any(NameIdentifier.class), any(String.class), any());
  }

  @Test
  public void testRegisterModelSetsOwnerWithNormalizedIdentifier() throws Exception {
    when(mockCatalogWrapper.capabilities()).thenReturn(new CaseInsensitiveCapability());

    NameIdentifier ident =
        NameIdentifier.of("test_metalake", "test_catalog", "TEST_SCHEMA", "MY_MODEL");
    Model mockModel = mock(Model.class);
    when(mockDispatcher.registerModel(any(NameIdentifier.class), any(String.class), any()))
        .thenReturn(mockModel);

    hookDispatcher.registerModel(ident, "comment", Collections.emptyMap());

    ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
    verify(mockOwnerDispatcher)
        .setOwner(eq("test_metalake"), captor.capture(), any(), eq(Owner.Type.USER));
    Assertions.assertEquals(
        "my_model",
        captor.getValue().name(),
        "Model name passed to setOwner must be lowercased by Capability.Scope.MODEL normalization");
    // MODEL scope is intentionally excluded from CapabilityHelpers.applyCapabilities(Namespace,
    // Scope, Capability), so the schema component in the namespace is NOT lowercased -- the
    // captured parent reflects exactly what ModelNormalizeDispatcher would also pass to the
    // manager. This assertion locks that behavior in.
    Assertions.assertEquals(
        "test_catalog.TEST_SCHEMA",
        captor.getValue().parent(),
        "Model parent must keep its schema component as-is: Capability.Scope.MODEL is excluded"
            + " from namespace normalization in CapabilityHelpers; if this changes, ownership"
            + " attachment will diverge from what ModelNormalizeDispatcher passes to the manager");
  }

  @Test
  public void testRegisterModelWithVersionPassesMetalakeAsFirstSetOwnerArg() {
    NameIdentifier ident =
        NameIdentifier.of("test_metalake", "test_catalog", "test_schema", "test_model");
    Model mockModel = mock(Model.class);
    when(mockDispatcher.registerModel(
            any(NameIdentifier.class), any(java.util.Map.class), any(String[].class), any(), any()))
        .thenReturn(mockModel);

    hookDispatcher.registerModel(
        ident,
        Collections.singletonMap("location", "s3://bucket/model"),
        new String[0],
        "comment",
        Collections.emptyMap());

    // Verify setOwner is called with the metalake name (level(0) of namespace), not the model
    // name. Previously this method incorrectly passed ident.name() as the first argument.
    ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
    verify(mockOwnerDispatcher)
        .setOwner(eq("test_metalake"), captor.capture(), any(), eq(Owner.Type.USER));
    Assertions.assertEquals(
        "test_model",
        captor.getValue().name(),
        "5-arg registerModel must pass the model name to setOwner unchanged when input is already"
            + " lowercase");
    Assertions.assertEquals(
        "test_catalog.test_schema",
        captor.getValue().parent(),
        "5-arg registerModel must build parent as <catalog>.<schema> (level(1).level(2)), not"
            + " level(0) or the model name; this regression test guards the previous bug where"
            + " ident.name() was used as the metalake arg");
  }

  private static class CaseInsensitiveCapability implements Capability {
    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("case-insensitive");
    }
  }
}
