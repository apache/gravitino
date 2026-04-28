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
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestSchemaHookDispatcher {

  private SchemaHookDispatcher hookDispatcher;
  private SchemaDispatcher mockDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;
  private CatalogManager mockCatalogManager;
  private CatalogManager.CatalogWrapper mockCatalogWrapper;
  // Save the originals before each test and restore them in tearDown so we do not leak null
  // state into the GravitinoEnv singleton across tests.
  private OwnerDispatcher savedOwnerDispatcher;
  private CatalogManager savedCatalogManager;

  @BeforeEach
  public void setUp() throws Exception {
    mockDispatcher = mock(SchemaDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
    mockCatalogManager = mock(CatalogManager.class);
    mockCatalogWrapper = mock(CatalogManager.CatalogWrapper.class);
    when(mockCatalogManager.loadCatalogAndWrap(any())).thenReturn(mockCatalogWrapper);
    when(mockCatalogWrapper.capabilities()).thenReturn(Capability.DEFAULT);
    savedOwnerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
    // Tests in this class that rely on the singleton catalogManager always go through
    // GravitinoEnv.getInstance().catalogManager(), but we cannot call the public accessor here
    // because it Preconditions-checks for non-null and would fail when GravitinoEnv has not been
    // initialized. Read the field directly via reflection to capture the current value safely.
    savedCatalogManager =
        (CatalogManager) FieldUtils.readField(GravitinoEnv.getInstance(), "catalogManager", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", mockCatalogManager, true);
    hookDispatcher = new SchemaHookDispatcher(mockDispatcher);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "ownerDispatcher", savedOwnerDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", savedCatalogManager, true);
  }

  @Test
  public void testCreateSchemaSucceedsEvenIfSetOwnerFails() {
    NameIdentifier ident = NameIdentifier.of("test_metalake", "test_catalog", "test_schema");
    Schema mockSchema = mock(Schema.class);
    when(mockDispatcher.createSchema(any(), any(), any())).thenReturn(mockSchema);

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    Schema result = hookDispatcher.createSchema(ident, "comment", Collections.emptyMap());

    Assertions.assertEquals(mockSchema, result);
    verify(mockDispatcher).createSchema(any(), any(), any());
  }

  @Test
  public void testCreateSchemaSetsOwnerWithNormalizedIdentifier() throws Exception {
    // Use a case-insensitive capability so the schema name is normalized to lower case before
    // setOwner is called, mirroring what NormalizeDispatcher would do for the manager.
    when(mockCatalogWrapper.capabilities()).thenReturn(new CaseInsensitiveCapability());

    NameIdentifier ident = NameIdentifier.of("test_metalake", "test_catalog", "MY_SCHEMA");
    Schema mockSchema = mock(Schema.class);
    when(mockDispatcher.createSchema(any(), any(), any())).thenReturn(mockSchema);

    hookDispatcher.createSchema(ident, "comment", Collections.emptyMap());

    ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
    verify(mockOwnerDispatcher)
        .setOwner(eq("test_metalake"), captor.capture(), any(), eq(Owner.Type.USER));
    Assertions.assertEquals(
        "my_schema",
        captor.getValue().name(),
        "Schema name passed to setOwner must be lowercased by Capability.Scope.SCHEMA"
            + " normalization");
    // Schema's namespace is [metalake, catalog]; NameIdentifierUtil.toMetadataObject uses
    // level(1) as parent. Catalog is not subject to per-scope name normalization here, so
    // parent is just the catalog name -- there is no schema component to normalize.
    Assertions.assertEquals(
        "test_catalog",
        captor.getValue().parent(),
        "Schema parent must be the catalog name (level(1) of the namespace); SCHEMA's namespace"
            + " has no schema component to normalize");
  }

  private static class CaseInsensitiveCapability implements Capability {
    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("case-insensitive");
    }
  }
}
