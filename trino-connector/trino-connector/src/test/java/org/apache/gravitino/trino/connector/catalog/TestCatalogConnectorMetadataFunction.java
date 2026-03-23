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
package org.apache.gravitino.trino.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.jupiter.api.Test;

public class TestCatalogConnectorMetadataFunction {

  @Test
  public void testSupportsFunctionsWhenCatalogSupports() {
    CatalogConnectorMetadata metadata = createMetadataWithFunctionCatalog(true);
    assertTrue(metadata.supportsFunctions());
  }

  @Test
  public void testSupportsFunctionsWhenCatalogDoesNotSupport() {
    CatalogConnectorMetadata metadata = createMetadataWithFunctionCatalog(false);
    assertFalse(metadata.supportsFunctions());
  }

  @Test
  public void testListFunctionInfosReturnsFunctions() {
    Function mockFunction = createMockFunction("test_func");
    FunctionCatalog functionCatalog = mock(FunctionCatalog.class);
    when(functionCatalog.listFunctionInfos(any(Namespace.class)))
        .thenReturn(new Function[] {mockFunction});

    CatalogConnectorMetadata metadata = createMetadataWithMockFunctionCatalog(functionCatalog);
    Function[] functions = metadata.listFunctionInfos("test_schema");
    assertEquals(1, functions.length);
    assertEquals("test_func", functions[0].name());
  }

  @Test
  public void testListFunctionInfosWhenUnsupported() {
    CatalogConnectorMetadata metadata = createMetadataWithFunctionCatalog(false);
    Function[] functions = metadata.listFunctionInfos("test_schema");
    assertEquals(0, functions.length);
  }

  @Test
  public void testGetFunctionReturnsFunction() {
    Function mockFunction = createMockFunction("my_func");
    FunctionCatalog functionCatalog = mock(FunctionCatalog.class);
    when(functionCatalog.getFunction(any(NameIdentifier.class))).thenReturn(mockFunction);

    CatalogConnectorMetadata metadata = createMetadataWithMockFunctionCatalog(functionCatalog);
    Function function = metadata.getFunction("test_schema", "my_func");
    assertNotNull(function);
    assertEquals("my_func", function.name());
  }

  @Test
  public void testGetFunctionReturnsNullWhenNotFound() {
    FunctionCatalog functionCatalog = mock(FunctionCatalog.class);
    when(functionCatalog.getFunction(any(NameIdentifier.class)))
        .thenThrow(new NoSuchFunctionException("Function does not exist"));

    CatalogConnectorMetadata metadata = createMetadataWithMockFunctionCatalog(functionCatalog);
    Function function = metadata.getFunction("test_schema", "no_such_func");
    assertNull(function);
  }

  @Test
  public void testGetFunctionReturnsNullWhenUnsupported() {
    CatalogConnectorMetadata metadata = createMetadataWithFunctionCatalog(false);
    Function function = metadata.getFunction("test_schema", "my_func");
    assertNull(function);
  }

  private CatalogConnectorMetadata createMetadataWithFunctionCatalog(boolean supportsFunctions) {
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog catalog = mock(Catalog.class);
    when(catalog.name()).thenReturn("test_catalog");
    when(metalake.loadCatalog(anyString())).thenReturn(catalog);
    when(catalog.asSchemas()).thenReturn(mock(SupportsSchemas.class));
    when(catalog.asTableCatalog()).thenReturn(mock(TableCatalog.class));
    if (supportsFunctions) {
      when(catalog.asFunctionCatalog()).thenReturn(mock(FunctionCatalog.class));
    } else {
      when(catalog.asFunctionCatalog())
          .thenThrow(new UnsupportedOperationException("Not supported"));
    }
    return new CatalogConnectorMetadata(metalake, NameIdentifier.of("metalake", "test_catalog"));
  }

  private CatalogConnectorMetadata createMetadataWithMockFunctionCatalog(
      FunctionCatalog functionCatalog) {
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog catalog = mock(Catalog.class);
    when(catalog.name()).thenReturn("test_catalog");
    when(metalake.loadCatalog(anyString())).thenReturn(catalog);
    when(catalog.asSchemas()).thenReturn(mock(SupportsSchemas.class));
    when(catalog.asTableCatalog()).thenReturn(mock(TableCatalog.class));
    when(catalog.asFunctionCatalog()).thenReturn(functionCatalog);
    return new CatalogConnectorMetadata(metalake, NameIdentifier.of("metalake", "test_catalog"));
  }

  private Function createMockFunction(String name) {
    Function function = mock(Function.class);
    when(function.name()).thenReturn(name);
    when(function.functionType()).thenReturn(FunctionType.SCALAR);
    when(function.deterministic()).thenReturn(true);
    when(function.definitions()).thenReturn(new FunctionDefinition[0]);
    Audit audit = mock(Audit.class);
    when(audit.creator()).thenReturn("test");
    when(audit.createTime()).thenReturn(Instant.now());
    when(function.auditInfo()).thenReturn(audit);
    return function;
  }
}
