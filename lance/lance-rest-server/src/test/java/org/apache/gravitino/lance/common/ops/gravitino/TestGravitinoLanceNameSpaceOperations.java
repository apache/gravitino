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
package org.apache.gravitino.lance.common.ops.gravitino;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.regex.Pattern;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.ListNamespacesResponse;
import org.mockito.Mockito;

class TestGravitinoLanceNameSpaceOperations {

  private static final String DELIMITER = Pattern.quote(".");
  private static final String LAKEHOUSE_PROVIDER = "lakehouse-generic";

  @Test
  void testListNamespacesOnSchemaReturnsEmptyList() {
    GravitinoLanceNamespaceWrapper namespaceWrapper =
        Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    Catalog catalog = Mockito.mock(Catalog.class);
    when(namespaceWrapper.loadAndValidateLakehouseCatalog("catalog")).thenReturn(catalog);
    when(namespaceWrapper.schemaExists(catalog, "schema")).thenReturn(true);
    GravitinoLanceNameSpaceOperations operations =
        new GravitinoLanceNameSpaceOperations(namespaceWrapper);

    ListNamespacesResponse response =
        operations.listNamespaces("catalog.schema", DELIMITER, null, null);

    Assertions.assertTrue(response.getNamespaces().isEmpty());
    Assertions.assertNull(response.getPageToken());
  }

  @Test
  void testListNamespacesOnNonExistentSchemaThrows() {
    GravitinoLanceNamespaceWrapper namespaceWrapper =
        Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    Catalog catalog = Mockito.mock(Catalog.class);
    when(namespaceWrapper.loadAndValidateLakehouseCatalog("catalog")).thenReturn(catalog);
    when(namespaceWrapper.schemaExists(catalog, "bogus_schema")).thenReturn(false);
    GravitinoLanceNameSpaceOperations operations =
        new GravitinoLanceNameSpaceOperations(namespaceWrapper);

    NamespaceNotFoundException exception =
        Assertions.assertThrows(
            NamespaceNotFoundException.class,
            () -> operations.listNamespaces("catalog.bogus_schema", DELIMITER, null, null));
    Assertions.assertEquals("Schema not found: bogus_schema", exception.getMessage());
    Assertions.assertEquals("bogus_schema", exception.getInstance());
  }

  @Test
  void testListNamespacesOnNonExistentCatalogThrows() {
    GravitinoLanceNamespaceWrapper namespaceWrapper =
        Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    when(namespaceWrapper.loadAndValidateLakehouseCatalog("bogus_catalog"))
        .thenThrow(
            new NamespaceNotFoundException(
                "Catalog not found: bogus_catalog", "", "bogus_catalog"));
    GravitinoLanceNameSpaceOperations operations =
        new GravitinoLanceNameSpaceOperations(namespaceWrapper);

    // A nonexistent catalog must be reported at every depth, not only for its own level.
    NamespaceNotFoundException exception =
        Assertions.assertThrows(
            NamespaceNotFoundException.class,
            () -> operations.listNamespaces("bogus_catalog.bogus_schema", DELIMITER, null, null));
    Assertions.assertEquals("Catalog not found: bogus_catalog", exception.getMessage());

    exception =
        Assertions.assertThrows(
            NamespaceNotFoundException.class,
            () -> operations.listNamespaces("bogus_catalog", DELIMITER, null, null));
    Assertions.assertEquals("Catalog not found: bogus_catalog", exception.getMessage());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"restrict", "cascade"})
  void testDropCatalogRefusesNonLakehouseCatalog(String behavior) {
    // A Hive catalog is invisible to Lance list, describe and exists, so Lance must not drop it.
    // The default (null) behavior is RESTRICT, which would otherwise reach the catalog hook's
    // privilege cleanup before the drop is refused as "in use".
    GravitinoLanceNamespaceWrapper.CatalogOperator operator = mockOperator();
    Catalog catalog = mockCatalog(Catalog.Type.RELATIONAL, "hive");
    when(operator.loadCatalog("sales")).thenReturn(catalog);
    GravitinoLanceNameSpaceOperations operations = newOperations(operator);

    NamespaceNotFoundException exception =
        Assertions.assertThrows(
            NamespaceNotFoundException.class,
            () -> operations.dropNamespace("sales", DELIMITER, "fail", behavior));

    Assertions.assertEquals("Catalog is not a lakehouse catalog: sales", exception.getMessage());
    Assertions.assertEquals("sales", exception.getInstance());
    verify(operator, never()).dropCatalog(anyString(), anyBoolean());
  }

  @ParameterizedTest
  @CsvSource({
    "RELATIONAL, lakehouse-iceberg",
    "RELATIONAL, jdbc-mysql",
    "FILESET, fileset",
    "FILESET, lakehouse-generic",
    "MODEL, lakehouse-generic"
  })
  void testDropCatalogRefusesEveryNonLakehouseKind(Catalog.Type type, String provider) {
    // Both the type and the provider must match. The right provider on the wrong type is refused.
    GravitinoLanceNamespaceWrapper.CatalogOperator operator = mockOperator();
    Catalog catalog = mockCatalog(type, provider);
    when(operator.loadCatalog("other")).thenReturn(catalog);
    GravitinoLanceNameSpaceOperations operations = newOperations(operator);

    Assertions.assertThrows(
        NamespaceNotFoundException.class,
        () -> operations.dropNamespace("other", DELIMITER, "fail", "cascade"));
    verify(operator, never()).dropCatalog(anyString(), anyBoolean());
  }

  @Test
  void testDropCatalogRefusesNonLakehouseCatalogInSkipMode() {
    // SKIP only tolerates a missing catalog. An existing non-lakehouse catalog is still refused,
    // as it is for a schema-level drop under a non-lakehouse catalog.
    GravitinoLanceNamespaceWrapper.CatalogOperator operator = mockOperator();
    Catalog catalog = mockCatalog(Catalog.Type.RELATIONAL, "hive");
    when(operator.loadCatalog("sales")).thenReturn(catalog);
    GravitinoLanceNameSpaceOperations operations = newOperations(operator);

    Assertions.assertThrows(
        NamespaceNotFoundException.class,
        () -> operations.dropNamespace("sales", DELIMITER, "skip", "cascade"));
    verify(operator, never()).dropCatalog(anyString(), anyBoolean());
  }

  @Test
  void testDropCatalogErrorMatchesDescribeForNonLakehouseCatalog() {
    GravitinoLanceNamespaceWrapper.CatalogOperator operator = mockOperator();
    Catalog catalog = mockCatalog(Catalog.Type.RELATIONAL, "hive");
    when(operator.loadCatalog("sales")).thenReturn(catalog);
    GravitinoLanceNameSpaceOperations operations = newOperations(operator);

    NamespaceNotFoundException describeException =
        Assertions.assertThrows(
            NamespaceNotFoundException.class,
            () -> operations.describeNamespace("sales", DELIMITER));
    Exception dropException =
        Assertions.assertThrows(
            Exception.class, () -> operations.dropNamespace("sales", DELIMITER, null, null));

    Assertions.assertEquals(describeException.getClass(), dropException.getClass());
    Assertions.assertEquals(describeException.getMessage(), dropException.getMessage());
    Assertions.assertEquals(
        describeException.getInstance(),
        ((NamespaceNotFoundException) dropException).getInstance());
  }

  @ParameterizedTest
  @CsvSource({"restrict, false", "cascade, true"})
  void testDropLakehouseCatalogStillDrops(String behavior, boolean expectedForce) {
    GravitinoLanceNamespaceWrapper.CatalogOperator operator = mockOperator();
    Catalog catalog = mockCatalog(Catalog.Type.RELATIONAL, LAKEHOUSE_PROVIDER);
    when(operator.loadCatalog("lance_cat")).thenReturn(catalog);
    when(operator.dropCatalog("lance_cat", expectedForce)).thenReturn(true);
    GravitinoLanceNameSpaceOperations operations = newOperations(operator);

    DropNamespaceResponse response =
        operations.dropNamespace("lance_cat", DELIMITER, "fail", behavior);

    Assertions.assertNotNull(response);
    verify(operator).dropCatalog("lance_cat", expectedForce);
  }

  @Test
  void testDropLakehouseCatalogInUseKeepsExistingError() {
    GravitinoLanceNamespaceWrapper.CatalogOperator operator = mockOperator();
    Catalog catalog = mockCatalog(Catalog.Type.RELATIONAL, LAKEHOUSE_PROVIDER);
    when(operator.loadCatalog("lance_cat")).thenReturn(catalog);
    when(operator.dropCatalog("lance_cat", false))
        .thenThrow(new CatalogInUseException("Catalog %s is in use", "lance_cat"));
    GravitinoLanceNameSpaceOperations operations = newOperations(operator);

    InvalidInputException exception =
        Assertions.assertThrows(
            InvalidInputException.class,
            () -> operations.dropNamespace("lance_cat", DELIMITER, null, null));
    Assertions.assertEquals("Catalog lance_cat is in use", exception.getMessage());
  }

  @Test
  void testDropMissingCatalogKeepsExistingBehavior() {
    GravitinoLanceNamespaceWrapper.CatalogOperator operator = mockOperator();
    when(operator.loadCatalog("bogus"))
        .thenThrow(new NoSuchCatalogException("Catalog %s does not exist", "bogus"));
    when(operator.dropCatalog(anyString(), anyBoolean())).thenReturn(false);
    GravitinoLanceNameSpaceOperations operations = newOperations(operator);

    NamespaceNotFoundException exception =
        Assertions.assertThrows(
            NamespaceNotFoundException.class,
            () -> operations.dropNamespace("bogus", DELIMITER, "fail", "restrict"));
    Assertions.assertEquals("Catalog not found: bogus", exception.getMessage());
    Assertions.assertEquals("bogus", exception.getInstance());

    Assertions.assertNotNull(operations.dropNamespace("bogus", DELIMITER, "skip", "cascade"));
  }

  private static GravitinoLanceNamespaceWrapper.CatalogOperator mockOperator() {
    // By default the backing drop succeeds, as it would for any catalog the caller owns.
    GravitinoLanceNamespaceWrapper.CatalogOperator operator =
        Mockito.mock(GravitinoLanceNamespaceWrapper.CatalogOperator.class);
    when(operator.dropCatalog(anyString(), anyBoolean())).thenReturn(true);
    return operator;
  }

  private static GravitinoLanceNameSpaceOperations newOperations(
      GravitinoLanceNamespaceWrapper.CatalogOperator operator) {
    // A real wrapper, so the scope check under test is the production one.
    GravitinoLanceNamespaceWrapper wrapper = new GravitinoLanceNamespaceWrapper();
    wrapper.setCatalogOperator(operator);
    return new GravitinoLanceNameSpaceOperations(wrapper);
  }

  private static Catalog mockCatalog(Catalog.Type type, String provider) {
    Catalog catalog = Mockito.mock(Catalog.class);
    when(catalog.type()).thenReturn(type);
    when(catalog.provider()).thenReturn(provider);
    return catalog;
  }
}
