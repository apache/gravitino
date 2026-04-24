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

  @BeforeEach
  public void setUp() throws Exception {
    mockDispatcher = mock(ModelDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
    mockCatalogManager = mock(CatalogManager.class);
    mockCatalogWrapper = mock(CatalogManager.CatalogWrapper.class);
    when(mockCatalogManager.loadCatalogAndWrap(any())).thenReturn(mockCatalogWrapper);
    when(mockCatalogWrapper.capabilities()).thenReturn(Capability.DEFAULT);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", mockCatalogManager, true);
    hookDispatcher = new ModelHookDispatcher(mockDispatcher);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", null, true);
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
        NameIdentifier.of("test_metalake", "test_catalog", "test_schema", "MY_MODEL");
    Model mockModel = mock(Model.class);
    when(mockDispatcher.registerModel(any(NameIdentifier.class), any(String.class), any()))
        .thenReturn(mockModel);

    hookDispatcher.registerModel(ident, "comment", Collections.emptyMap());

    ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
    verify(mockOwnerDispatcher)
        .setOwner(eq("test_metalake"), captor.capture(), any(), eq(Owner.Type.USER));
    Assertions.assertEquals("my_model", captor.getValue().name());
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
    verify(mockOwnerDispatcher).setOwner(eq("test_metalake"), any(), any(), eq(Owner.Type.USER));
  }

  private static class CaseInsensitiveCapability implements Capability {
    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("case-insensitive");
    }
  }
}
