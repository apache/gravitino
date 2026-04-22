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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestIcebergCatalogOperations {

  private static final String SEPARATOR = ":";
  private static final String METALAKE = "metalake";
  private static final String CATALOG = "catalog";

  private IcebergCatalogOperations catalogOperations;
  private IcebergCatalogWrapper mockWrapper;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    Config mockConfig = mock(Config.class);
    when(mockConfig.get(Configs.SCHEMA_NAMESPACE_SEPARATOR)).thenReturn(SEPARATOR);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", mockConfig, true);

    mockWrapper = mock(IcebergCatalogWrapper.class);
    catalogOperations = new IcebergCatalogOperations();
    FieldUtils.writeField(catalogOperations, "icebergCatalogWrapper", mockWrapper, true);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", null, true);
  }

  // ─── existing test ───────────────────────────────────────────────────────────

  @Test
  public void testTestConnection() {
    IcebergCatalogOperations ops = new IcebergCatalogOperations();
    Exception exception =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                ops.testConnection(
                    NameIdentifier.of(METALAKE, CATALOG),
                    Catalog.Type.RELATIONAL,
                    "iceberg",
                    "comment",
                    ImmutableMap.of()));
    Assertions.assertTrue(
        exception.getMessage().contains("Failed to run listNamespace on Iceberg catalog"));
  }

  // ─── createSchema ────────────────────────────────────────────────────────────

  @Test
  public void testCreateFlatSchemaCreatesSimpleNamespace() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "myschema");
    CreateNamespaceResponse mockResponse = mock(CreateNamespaceResponse.class);
    when(mockWrapper.createNamespace(any())).thenReturn(mockResponse);

    IcebergSchema schema = catalogOperations.createSchema(ident, "comment", Collections.emptyMap());

    ArgumentCaptor<CreateNamespaceRequest> captor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockWrapper).createNamespace(captor.capture());
    org.apache.iceberg.catalog.Namespace capturedNs = captor.getValue().namespace();
    Assertions.assertArrayEquals(new String[] {"myschema"}, capturedNs.levels());
    Assertions.assertEquals("myschema", schema.name());
  }

  @Test
  public void testCreateNestedSchemaCreatesMultiLevelNamespace() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "A:B:C");
    CreateNamespaceResponse mockResponse = mock(CreateNamespaceResponse.class);
    when(mockWrapper.createNamespace(any())).thenReturn(mockResponse);

    IcebergSchema schema = catalogOperations.createSchema(ident, "comment", Collections.emptyMap());

    ArgumentCaptor<CreateNamespaceRequest> captor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockWrapper).createNamespace(captor.capture());
    org.apache.iceberg.catalog.Namespace capturedNs = captor.getValue().namespace();
    // Logical "A:B:C" with separator ":" → Iceberg Namespace.of("A","B","C")
    Assertions.assertArrayEquals(new String[] {"A", "B", "C"}, capturedNs.levels());
    Assertions.assertEquals("A:B:C", schema.name());
  }

  @Test
  public void testCreateTwoLevelSchemaCreatesCorrectNamespace() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "team:sales");
    CreateNamespaceResponse mockResponse = mock(CreateNamespaceResponse.class);
    when(mockWrapper.createNamespace(any())).thenReturn(mockResponse);

    catalogOperations.createSchema(ident, null, Collections.emptyMap());

    ArgumentCaptor<CreateNamespaceRequest> captor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockWrapper).createNamespace(captor.capture());
    Assertions.assertArrayEquals(new String[] {"team", "sales"}, captor.getValue().namespace().levels());
  }

  // ─── loadSchema ──────────────────────────────────────────────────────────────

  @Test
  public void testLoadFlatSchemaLoadsSimpleNamespace() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "myschema");
    Map<String, String> props = new HashMap<>();
    props.put("comment", "test comment");
    GetNamespaceResponse mockResponse =
        GetNamespaceResponse.builder()
            .withNamespace(org.apache.iceberg.catalog.Namespace.of("myschema"))
            .setProperties(props)
            .build();
    when(mockWrapper.loadNamespace(org.apache.iceberg.catalog.Namespace.of("myschema")))
        .thenReturn(mockResponse);

    IcebergSchema schema = catalogOperations.loadSchema(ident);

    verify(mockWrapper).loadNamespace(org.apache.iceberg.catalog.Namespace.of("myschema"));
    Assertions.assertEquals("myschema", schema.name());
  }

  @Test
  public void testLoadNestedSchemaLoadsMultiLevelNamespace() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "A:B:C");
    org.apache.iceberg.catalog.Namespace expectedNs =
        org.apache.iceberg.catalog.Namespace.of("A", "B", "C");
    Map<String, String> props = Collections.emptyMap();
    GetNamespaceResponse mockResponse =
        GetNamespaceResponse.builder().withNamespace(expectedNs).setProperties(props).build();
    when(mockWrapper.loadNamespace(expectedNs)).thenReturn(mockResponse);

    IcebergSchema schema = catalogOperations.loadSchema(ident);

    verify(mockWrapper).loadNamespace(expectedNs);
    Assertions.assertEquals("A:B:C", schema.name());
  }

  // ─── dropSchema ──────────────────────────────────────────────────────────────

  @Test
  public void testDropFlatSchemaDropsSimpleNamespace() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "myschema");

    boolean result = catalogOperations.dropSchema(ident, false);

    verify(mockWrapper).dropNamespace(org.apache.iceberg.catalog.Namespace.of("myschema"));
    Assertions.assertTrue(result);
  }

  @Test
  public void testDropNestedSchemaDropsMultiLevelNamespace() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "A:B:C");
    org.apache.iceberg.catalog.Namespace expectedNs =
        org.apache.iceberg.catalog.Namespace.of("A", "B", "C");

    boolean result = catalogOperations.dropSchema(ident, false);

    verify(mockWrapper).dropNamespace(expectedNs);
    Assertions.assertTrue(result);
  }

  // ─── listSchemas ─────────────────────────────────────────────────────────────

  @Test
  public void testListSchemasConvertsMultiLevelNamespacesToLogicalNames() {
    Namespace gravitinoNs = Namespace.of(METALAKE, CATALOG);
    org.apache.iceberg.catalog.Namespace flatNs = org.apache.iceberg.catalog.Namespace.of("mydb");
    org.apache.iceberg.catalog.Namespace nestedNs =
        org.apache.iceberg.catalog.Namespace.of("A", "B", "C");
    ListNamespacesResponse mockResponse =
        ListNamespacesResponse.builder().addAll(Arrays.asList(flatNs, nestedNs)).build();
    when(mockWrapper.listNamespace(any())).thenReturn(mockResponse);

    NameIdentifier[] result = catalogOperations.listSchemas(gravitinoNs);

    Assertions.assertEquals(2, result.length);
    // Flat namespace stays as-is
    Assertions.assertTrue(
        Arrays.stream(result).anyMatch(id -> "mydb".equals(id.name())));
    // Multi-level Iceberg namespace is joined with the configured separator
    Assertions.assertTrue(
        Arrays.stream(result).anyMatch(id -> "A:B:C".equals(id.name())));
  }

  @Test
  public void testListSchemasFlatOnlyReturnsUnchangedNames() {
    Namespace gravitinoNs = Namespace.of(METALAKE, CATALOG);
    org.apache.iceberg.catalog.Namespace ns1 = org.apache.iceberg.catalog.Namespace.of("db1");
    org.apache.iceberg.catalog.Namespace ns2 = org.apache.iceberg.catalog.Namespace.of("db2");
    ListNamespacesResponse mockResponse =
        ListNamespacesResponse.builder().addAll(Arrays.asList(ns1, ns2)).build();
    when(mockWrapper.listNamespace(any())).thenReturn(mockResponse);

    NameIdentifier[] result = catalogOperations.listSchemas(gravitinoNs);

    Assertions.assertEquals(2, result.length);
    Assertions.assertTrue(Arrays.stream(result).anyMatch(id -> "db1".equals(id.name())));
    Assertions.assertTrue(Arrays.stream(result).anyMatch(id -> "db2".equals(id.name())));
  }
}
