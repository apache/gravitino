/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.common.ops.gravitino;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.lance.common.ops.LanceMetadataFilter;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesResponse;
import org.mockito.Mockito;

/** Verifies that unauthorized namespaces never consume a slot in the returned page. */
class TestGravitinoLanceNamespaceListFiltering {

  private static final String DELIMITER = Pattern.quote("$");
  private static final String CATALOG = "b_catalog";
  private static final String SCHEMA = "b_schema";

  @Test
  void testCatalogsAreFilteredBeforePagination() {
    GravitinoLanceNamespaceWrapper wrapper = Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    Catalog hidden = catalog("a_catalog");
    Catalog visible = catalog(CATALOG);
    Mockito.when(wrapper.listCatalogsInfo()).thenReturn(new Catalog[] {hidden, visible});
    Mockito.when(wrapper.isLakehouseCatalog(Mockito.any())).thenReturn(true);
    // "a_catalog" sorts first, so it would fill the single-entry page if it were filtered after
    // pagination.
    Mockito.doReturn(allowOnly(CATALOG)).when(wrapper).metadataFilter();

    ListNamespacesResponse response =
        new GravitinoLanceNameSpaceOperations(wrapper).listNamespaces("", DELIMITER, null, 1);

    Assertions.assertEquals(Set.of(CATALOG), response.getNamespaces());

    Mockito.when(wrapper.metadataFilter()).thenReturn(LanceMetadataFilter.NOOP);
    response =
        new GravitinoLanceNameSpaceOperations(wrapper).listNamespaces("", DELIMITER, null, 10);
    Assertions.assertEquals(Set.of("a_catalog", CATALOG), response.getNamespaces());
  }

  @Test
  void testSchemasAreFilteredBeforePagination() {
    GravitinoLanceNamespaceWrapper wrapper = Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    Catalog catalog = catalog(CATALOG);
    Mockito.when(wrapper.loadAndValidateLakehouseCatalog(CATALOG)).thenReturn(catalog);
    Mockito.when(wrapper.listSchemas(catalog)).thenReturn(new String[] {"a_schema", "b_schema"});
    Mockito.doReturn(allowOnly("b_schema")).when(wrapper).metadataFilter();

    ListNamespacesResponse response =
        new GravitinoLanceNameSpaceOperations(wrapper).listNamespaces(CATALOG, DELIMITER, null, 1);

    Assertions.assertEquals(Set.of("b_schema"), response.getNamespaces());
  }

  @Test
  void testTablesAreFilteredBeforePagination() {
    GravitinoLanceNamespaceWrapper wrapper = Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    Catalog catalog = catalog(CATALOG);
    Mockito.when(wrapper.loadAndValidateLakehouseCatalog(CATALOG)).thenReturn(catalog);
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(wrapper.asTableCatalog(catalog)).thenReturn(tableCatalog);
    Mockito.when(tableCatalog.listTables(Namespace.of(SCHEMA)))
        .thenReturn(
            new NameIdentifier[] {
              NameIdentifier.of(SCHEMA, "a_table"), NameIdentifier.of(SCHEMA, "b_table")
            });
    // "a_table" sorts first, so it would fill the single-entry page if it were filtered after
    // pagination.
    Mockito.doReturn(allowOnly("b_table")).when(wrapper).metadataFilter();

    ListTablesResponse response =
        new GravitinoLanceNameSpaceOperations(wrapper)
            .listTables(CATALOG + "$" + SCHEMA, "$", null, 1);

    Assertions.assertEquals(Set.of("b_table"), Set.copyOf(response.getTables()));

    Mockito.when(wrapper.metadataFilter()).thenReturn(LanceMetadataFilter.NOOP);
    response =
        new GravitinoLanceNameSpaceOperations(wrapper)
            .listTables(CATALOG + "$" + SCHEMA, "$", null, 10);
    Assertions.assertEquals(Set.of("a_table", "b_table"), Set.copyOf(response.getTables()));
  }

  @Test
  void testNullFilterRestoresNoop() {
    GravitinoLanceNamespaceWrapper wrapper = new GravitinoLanceNamespaceWrapper();
    wrapper.setMetadataFilter(allowOnly(CATALOG));
    wrapper.setMetadataFilter(null);

    Assertions.assertSame(LanceMetadataFilter.NOOP, wrapper.metadataFilter());
  }

  private Catalog catalog(String name) {
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.when(catalog.name()).thenReturn(name);
    return catalog;
  }

  private LanceMetadataFilter allowOnly(String allowedName) {
    LanceMetadataFilter filter = Mockito.mock(LanceMetadataFilter.class);
    Mockito.when(filter.filterCatalogs(Mockito.anyList()))
        .thenAnswer(invocation -> retain(invocation.getArgument(0), allowedName));
    Mockito.when(filter.filterSchemas(Mockito.anyString(), Mockito.anyList()))
        .thenAnswer(invocation -> retain(invocation.getArgument(1), allowedName));
    Mockito.when(filter.filterTables(Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
        .thenAnswer(invocation -> retain(invocation.getArgument(2), allowedName));
    return filter;
  }

  private List<String> retain(List<String> names, String allowedName) {
    return names.stream().filter(allowedName::equals).toList();
  }
}
