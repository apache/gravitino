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
package org.apache.gravitino.iceberg.service.dispatcher;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link RegisterTableSourceResolver}. */
class TestRegisterTableSourceResolver {

  private static final String META_LOC = "file:/warehouse/other/secret/metadata/v1.metadata.json";
  private static final String TABLE_LOC = "file:/warehouse/other/secret";

  @Test
  void testFindsForeignSourceByMetadataLocation() {
    IcebergCatalogWrapper catalog = mock(IcebergCatalogWrapper.class);
    TableIdentifier source = TableIdentifier.of("other", "secret");
    TableIdentifier target = TableIdentifier.of("mine", "copy");
    stubCatalogWithSource(catalog, source);
    when(catalog.getTableMetadataLocation(source)).thenReturn(Optional.of(META_LOC));

    List<TableIdentifier> found =
        RegisterTableSourceResolver.findSourceTables(catalog, target, META_LOC, tableMetadata());

    Assertions.assertEquals(List.of(source), found);
  }

  @Test
  void testIgnoresTargetItself() {
    IcebergCatalogWrapper catalog = mock(IcebergCatalogWrapper.class);
    TableIdentifier target = TableIdentifier.of("mine", "t");
    stubCatalogWithSource(catalog, target);
    when(catalog.getTableMetadataLocation(target)).thenReturn(Optional.of(META_LOC));

    List<TableIdentifier> found =
        RegisterTableSourceResolver.findSourceTables(catalog, target, META_LOC, tableMetadata());

    Assertions.assertTrue(found.isEmpty());
  }

  @Test
  void testSkipsNamespaceDroppedDuringScan() {
    IcebergCatalogWrapper catalog = mock(IcebergCatalogWrapper.class);
    TableIdentifier source = TableIdentifier.of("other", "secret");
    stubCatalogWithSource(catalog, source);
    when(catalog.listNamespace(source.namespace()))
        .thenThrow(new NoSuchNamespaceException("Namespace does not exist: other"));

    List<TableIdentifier> found =
        RegisterTableSourceResolver.findSourceTables(
            catalog, TableIdentifier.of("mine", "copy"), META_LOC, tableMetadata());

    Assertions.assertTrue(found.isEmpty());
  }

  @Test
  void testUnreadableSourceMetadataPropagates() {
    IcebergCatalogWrapper catalog = mock(IcebergCatalogWrapper.class);
    TableIdentifier source = TableIdentifier.of("other", "secret");
    stubCatalogWithSource(catalog, source);
    // The live table moved on to a corrupt current metadata file; the caller registers an older
    // one.
    when(catalog.getTableMetadataLocation(source))
        .thenReturn(Optional.of("file:/warehouse/other/secret/metadata/v2.metadata.json"));
    when(catalog.loadTableMetadata(source)).thenThrow(new RuntimeException("corrupt metadata"));

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            RegisterTableSourceResolver.findSourceTables(
                catalog, TableIdentifier.of("mine", "copy"), META_LOC, tableMetadata()));
  }

  @Test
  void testNormalizePathUnifiesFileUriForms() {
    Assertions.assertEquals(
        "file:/warehouse/t", RegisterTableSourceResolver.normalizePath("file:///warehouse/t"));
    Assertions.assertEquals(
        "file:/warehouse/t", RegisterTableSourceResolver.normalizePath("file:/warehouse/t"));
  }

  private static void stubCatalogWithSource(IcebergCatalogWrapper catalog, TableIdentifier source) {
    Namespace ns = source.namespace();
    ListNamespacesResponse rootNs = mock(ListNamespacesResponse.class);
    when(rootNs.namespaces()).thenReturn(List.of(ns));
    ListNamespacesResponse childNs = mock(ListNamespacesResponse.class);
    when(childNs.namespaces()).thenReturn(Collections.emptyList());
    ListTablesResponse childTables = mock(ListTablesResponse.class);
    when(childTables.identifiers()).thenReturn(List.of(source));

    when(catalog.listNamespace(Namespace.empty())).thenReturn(rootNs);
    when(catalog.listNamespace(ns)).thenReturn(childNs);
    // Like Iceberg's HiveCatalog, the root namespace cannot list tables.
    when(catalog.listTable(Namespace.empty()))
        .thenThrow(new IllegalArgumentException("Missing database in namespace: "));
    when(catalog.listTable(ns)).thenReturn(childTables);
  }

  private static TableMetadata tableMetadata() {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), TABLE_LOC, Collections.emptyMap());
  }
}
