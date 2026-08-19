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

import static org.mockito.Mockito.when;

import java.util.regex.Pattern;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.SupportsSchemas;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.model.ListNamespacesResponse;
import org.mockito.Mockito;

class TestGravitinoLanceNameSpaceOperations {

  private static final String DELIMITER = Pattern.quote(".");

  @Test
  void testListNamespacesOnSchemaReturnsEmptyList() {
    GravitinoLanceNamespaceWrapper namespaceWrapper =
        Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    Catalog catalog = mockCatalogWithSchema("schema", true);
    when(namespaceWrapper.loadAndValidateLakehouseCatalog("catalog")).thenReturn(catalog);
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
    Catalog catalog = mockCatalogWithSchema("bogus_schema", false);
    when(namespaceWrapper.loadAndValidateLakehouseCatalog("catalog")).thenReturn(catalog);
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

  private static Catalog mockCatalogWithSchema(String schemaName, boolean exists) {
    Catalog catalog = Mockito.mock(Catalog.class);
    SupportsSchemas schemas = Mockito.mock(SupportsSchemas.class);
    when(catalog.asSchemas()).thenReturn(schemas);
    when(schemas.schemaExists(schemaName)).thenReturn(exists);
    return catalog;
  }
}
