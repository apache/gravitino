/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lance.common.ops.gravitino;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Schema;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;

class TestLanceNamespaceCreateAuthorization {

  @ParameterizedTest
  @ValueSource(strings = {"create", "exist_ok", "overwrite"})
  void testCreateIsAuthorizedSeparatelyFromLoad(String mode) {
    GravitinoLanceNamespaceWrapper wrapper = mock(GravitinoLanceNamespaceWrapper.class);
    Catalog catalog = mock(Catalog.class);
    Schema schema = mock(Schema.class);
    when(wrapper.loadCatalog("catalog")).thenThrow(new ForbiddenException("Cannot load catalog"));
    when(wrapper.createCatalog(eq("catalog"), any(), anyString(), anyString(), anyMap()))
        .thenReturn(catalog);
    when(wrapper.loadAndValidateLakehouseCatalog("catalog")).thenReturn(catalog);
    when(wrapper.loadSchema(catalog, "schema"))
        .thenThrow(new ForbiddenException("Cannot load schema"));
    when(wrapper.createSchema(eq(catalog), eq("schema"), isNull(), anyMap())).thenReturn(schema);
    GravitinoLanceNameSpaceOperations operations = new GravitinoLanceNameSpaceOperations(wrapper);
    Assertions.assertNotNull(operations.createNamespace("catalog", "\\.", mode, Map.of()));
    Assertions.assertNotNull(operations.createNamespace("catalog.schema", "\\.", mode, Map.of()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"create", "exist_ok", "overwrite"})
  void testHiddenExistingNamespacesKeepReadDenial(String mode) {
    GravitinoLanceNamespaceWrapper wrapper = mock(GravitinoLanceNamespaceWrapper.class);
    Catalog catalog = mock(Catalog.class);
    ForbiddenException denied = new ForbiddenException("Cannot load namespace");
    when(wrapper.loadCatalog("catalog")).thenThrow(denied);
    when(wrapper.createCatalog(eq("catalog"), any(), anyString(), anyString(), anyMap()))
        .thenThrow(new CatalogAlreadyExistsException("exists"));
    when(wrapper.loadAndValidateLakehouseCatalog("catalog")).thenReturn(catalog);
    when(wrapper.loadSchema(catalog, "schema")).thenThrow(denied);
    when(wrapper.createSchema(eq(catalog), eq("schema"), isNull(), anyMap()))
        .thenThrow(new SchemaAlreadyExistsException("exists"));
    GravitinoLanceNameSpaceOperations operations = new GravitinoLanceNameSpaceOperations(wrapper);
    Assertions.assertSame(
        denied,
        Assertions.assertThrows(
            ForbiddenException.class,
            () -> operations.createNamespace("catalog", "\\.", mode, Map.of())));
    Assertions.assertSame(
        denied,
        Assertions.assertThrows(
            ForbiddenException.class,
            () -> operations.createNamespace("catalog.schema", "\\.", mode, Map.of())));
    verify(wrapper, never()).alterCatalog(anyString(), any());
    verify(wrapper, never()).alterSchema(any(), anyString(), any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"catalog", "catalog.schema"})
  void testConcurrentCreateKeepsConflictStatus(String namespace) {
    GravitinoLanceNamespaceWrapper wrapper = mock(GravitinoLanceNamespaceWrapper.class);
    Catalog catalog = mock(Catalog.class);
    when(wrapper.loadCatalog("catalog")).thenThrow(new NoSuchCatalogException("missing"));
    when(wrapper.createCatalog(eq("catalog"), any(), anyString(), anyString(), anyMap()))
        .thenThrow(new CatalogAlreadyExistsException("exists"));
    when(wrapper.loadAndValidateLakehouseCatalog("catalog")).thenReturn(catalog);
    when(wrapper.loadSchema(catalog, "schema")).thenThrow(new NoSuchSchemaException("missing"));
    when(wrapper.createSchema(eq(catalog), eq("schema"), isNull(), anyMap()))
        .thenThrow(new SchemaAlreadyExistsException("exists"));
    Assertions.assertThrows(
        NamespaceAlreadyExistsException.class,
        () ->
            new GravitinoLanceNameSpaceOperations(wrapper)
                .createNamespace(namespace, "\\.", "create", Map.of()));
  }
}
