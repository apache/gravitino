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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.iceberg.exceptions.NoSuchWarehouseException;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogOperations {

  private static final String METALAKE = "metalake";
  private static final String CATALOG = "catalog";

  @Test
  public void testTestConnection() {
    IcebergCatalogOperations catalogOperations = new IcebergCatalogOperations();
    Exception exception =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                catalogOperations.testConnection(
                    NameIdentifier.of(METALAKE, CATALOG),
                    Catalog.Type.RELATIONAL,
                    "iceberg",
                    "comment",
                    ImmutableMap.of()));
    Assertions.assertTrue(
        exception.getMessage().contains("Failed to run listNamespace on Iceberg catalog"));
  }

  @Test
  public void testTranslateInitializationFailureRestUnresolvableWarehouse() {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND, "rest",
                IcebergConstants.WAREHOUSE, "s3://warehouse/"));
    RuntimeException cause =
        new NoSuchWarehouseException(
            "Couldn't find Iceberg configuration for catalog %s", "s3://warehouse/");

    RuntimeException translated =
        IcebergCatalogOperations.translateInitializationFailure(config, cause);

    Assertions.assertInstanceOf(IllegalArgumentException.class, translated);
    Assertions.assertSame(cause, translated.getCause());
    Assertions.assertTrue(
        translated.getMessage().contains("selects a catalog by name"), translated.getMessage());
    Assertions.assertTrue(
        translated.getMessage().contains("s3://warehouse/"), translated.getMessage());
  }

  @Test
  public void testTranslateInitializationFailureLeavesOtherFailuresUnchanged() {
    IcebergConfig restConfig =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND, "rest",
                IcebergConstants.WAREHOUSE, "s3://warehouse/"));
    RuntimeException unrelated = new RuntimeException("connection refused");
    Assertions.assertSame(
        unrelated, IcebergCatalogOperations.translateInitializationFailure(restConfig, unrelated));

    // Non-REST backends never get the hint, even for a warehouse-resolution cause.
    IcebergConfig hiveConfig =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND, "hive",
                IcebergConstants.WAREHOUSE, "s3://warehouse/"));
    RuntimeException noSuchWarehouse =
        new NoSuchWarehouseException("Couldn't find Iceberg configuration for catalog %s", "x");
    Assertions.assertSame(
        noSuchWarehouse,
        IcebergCatalogOperations.translateInitializationFailure(hiveConfig, noSuchWarehouse));

    // REST without a configured warehouse: nothing to hint about.
    IcebergConfig noWarehouseConfig =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "rest"));
    Assertions.assertSame(
        noSuchWarehouse,
        IcebergCatalogOperations.translateInitializationFailure(
            noWarehouseConfig, noSuchWarehouse));
  }

  @Test
  public void testTestConnectionNonRestWarehouseHasNoHint() {
    IcebergCatalogWrapper mockWrapper = mock(IcebergCatalogWrapper.class);
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND, "hive",
                IcebergConstants.WAREHOUSE, "s3://warehouse/"));
    when(mockWrapper.getIcebergConfig()).thenReturn(config);
    when(mockWrapper.listNamespace(any())).thenThrow(new RuntimeException("boom"));

    IcebergCatalogOperations catalogOperations = new IcebergCatalogOperations();
    catalogOperations.icebergCatalogWrapper = mockWrapper;

    Exception exception =
        Assertions.assertThrows(
            ConnectionFailedException.class,
            () ->
                catalogOperations.testConnection(
                    NameIdentifier.of(METALAKE, CATALOG),
                    Catalog.Type.RELATIONAL,
                    "iceberg",
                    "comment",
                    ImmutableMap.of()));
    Assertions.assertTrue(
        exception.getMessage().contains("Failed to run listNamespace on Iceberg catalog"),
        exception.getMessage());
    Assertions.assertFalse(
        exception.getMessage().contains("selects a catalog by name"), exception.getMessage());
  }

  @Test
  public void testListSchemasConvertsMultiLevelNamespacesToLogicalNames() {
    IcebergCatalogWrapper mockWrapper = mock(IcebergCatalogWrapper.class);
    IcebergCatalogOperations catalogOperations = new IcebergCatalogOperations();
    catalogOperations.icebergCatalogWrapper = mockWrapper;

    org.apache.iceberg.catalog.Namespace flatNs = org.apache.iceberg.catalog.Namespace.of("mydb");
    org.apache.iceberg.catalog.Namespace hierarchicalNs =
        org.apache.iceberg.catalog.Namespace.of("A", "B", "C");
    ListNamespacesResponse mockResponse =
        ListNamespacesResponse.builder().addAll(Arrays.asList(flatNs, hierarchicalNs)).build();
    when(mockWrapper.listNamespace(any())).thenReturn(mockResponse);

    NameIdentifier[] result = catalogOperations.listSchemas(Namespace.of(METALAKE, CATALOG));

    Assertions.assertEquals(2, result.length);
    // Flat namespace stays as-is.
    Assertions.assertTrue(Arrays.stream(result).anyMatch(id -> "mydb".equals(id.name())));
    // Multi-level Iceberg namespace is joined with the configured separator.
    Assertions.assertTrue(Arrays.stream(result).anyMatch(id -> "A:B:C".equals(id.name())));
  }

  @Test
  public void testListSchemasFlatOnlyReturnsUnchangedNames() {
    IcebergCatalogWrapper mockWrapper = mock(IcebergCatalogWrapper.class);
    IcebergCatalogOperations catalogOperations = new IcebergCatalogOperations();
    catalogOperations.icebergCatalogWrapper = mockWrapper;

    org.apache.iceberg.catalog.Namespace ns1 = org.apache.iceberg.catalog.Namespace.of("db1");
    org.apache.iceberg.catalog.Namespace ns2 = org.apache.iceberg.catalog.Namespace.of("db2");
    ListNamespacesResponse mockResponse =
        ListNamespacesResponse.builder().addAll(Arrays.asList(ns1, ns2)).build();
    when(mockWrapper.listNamespace(any())).thenReturn(mockResponse);

    NameIdentifier[] result = catalogOperations.listSchemas(Namespace.of(METALAKE, CATALOG));

    Assertions.assertEquals(2, result.length);
    Assertions.assertTrue(Arrays.stream(result).anyMatch(id -> "db1".equals(id.name())));
    Assertions.assertTrue(Arrays.stream(result).anyMatch(id -> "db2".equals(id.name())));
  }
}
