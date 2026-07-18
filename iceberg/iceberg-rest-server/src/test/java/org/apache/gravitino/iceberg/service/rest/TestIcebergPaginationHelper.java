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
package org.apache.gravitino.iceberg.service.rest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergPaginationHelper {

  @Test
  void testPaginateNamespacesNoPagination() {
    ListNamespacesResponse response =
        ListNamespacesResponse.builder().add(Namespace.of("ns1")).add(Namespace.of("ns2")).build();
    ListNamespacesResponse result =
        IcebergPaginationHelper.paginateNamespaces(response, Optional.empty(), Optional.empty());
    Assertions.assertEquals(2, result.namespaces().size());
    Assertions.assertNull(result.nextPageToken());
  }

  @Test
  void testPaginateNamespacesFirstPage() {
    ListNamespacesResponse response =
        ListNamespacesResponse.builder()
            .add(Namespace.of("ns1"))
            .add(Namespace.of("ns2"))
            .add(Namespace.of("ns3"))
            .build();
    ListNamespacesResponse result =
        IcebergPaginationHelper.paginateNamespaces(response, Optional.empty(), Optional.of(2));
    Assertions.assertEquals(2, result.namespaces().size());
    Assertions.assertNotNull(result.nextPageToken());
  }

  @Test
  void testPaginateNamespacesSecondPage() {
    ListNamespacesResponse response =
        ListNamespacesResponse.builder()
            .add(Namespace.of("ns1"))
            .add(Namespace.of("ns2"))
            .add(Namespace.of("ns3"))
            .build();
    // First page
    ListNamespacesResponse firstPage =
        IcebergPaginationHelper.paginateNamespaces(response, Optional.empty(), Optional.of(2));
    // Second page using cursor from first page
    ListNamespacesResponse secondPage =
        IcebergPaginationHelper.paginateNamespaces(
            response, Optional.ofNullable(firstPage.nextPageToken()), Optional.of(2));
    Assertions.assertEquals(1, secondPage.namespaces().size());
    Assertions.assertNull(secondPage.nextPageToken());
  }

  @Test
  void testPaginateNamespacesSortsDeterministically() {
    // Items added in reverse order should still paginate in sorted order
    ListNamespacesResponse response =
        ListNamespacesResponse.builder()
            .add(Namespace.of("ns3"))
            .add(Namespace.of("ns1"))
            .add(Namespace.of("ns2"))
            .build();
    ListNamespacesResponse result =
        IcebergPaginationHelper.paginateNamespaces(response, Optional.empty(), Optional.of(2));
    List<String> names =
        result.namespaces().stream().map(Namespace::toString).collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList("ns1", "ns2"), names);
  }

  @Test
  void testPaginateNamespacesCursorBeyondAllItems() {
    ListNamespacesResponse response =
        ListNamespacesResponse.builder().add(Namespace.of("ns1")).add(Namespace.of("ns2")).build();
    // Cursor after all items alphabetically
    ListNamespacesResponse result =
        IcebergPaginationHelper.paginateNamespaces(response, Optional.of("zzzz"), Optional.of(10));
    Assertions.assertEquals(0, result.namespaces().size());
    Assertions.assertNull(result.nextPageToken());
  }

  @Test
  void testPaginateNamespacesPageTokenWithoutPageSize() {
    ListNamespacesResponse response =
        ListNamespacesResponse.builder()
            .add(Namespace.of("ns1"))
            .add(Namespace.of("ns2"))
            .add(Namespace.of("ns3"))
            .build();
    // pageToken without pageSize returns all items after cursor
    ListNamespacesResponse result =
        IcebergPaginationHelper.paginateNamespaces(response, Optional.of("ns1"), Optional.empty());
    List<String> names =
        result.namespaces().stream().map(Namespace::toString).collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList("ns2", "ns3"), names);
    Assertions.assertNull(result.nextPageToken());
  }

  @Test
  void testPaginateNamespacesZeroPageSizeThrows() {
    ListNamespacesResponse response =
        ListNamespacesResponse.builder().add(Namespace.of("ns1")).build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergPaginationHelper.paginateNamespaces(response, Optional.empty(), Optional.of(0)));
  }

  @Test
  void testPaginateNamespacesNegativePageSizeThrows() {
    ListNamespacesResponse response =
        ListNamespacesResponse.builder().add(Namespace.of("ns1")).build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergPaginationHelper.paginateNamespaces(
                response, Optional.empty(), Optional.of(-1)));
  }

  @Test
  void testPaginateNamespacesEmptyList() {
    ListNamespacesResponse response = ListNamespacesResponse.builder().build();
    ListNamespacesResponse result =
        IcebergPaginationHelper.paginateNamespaces(response, Optional.empty(), Optional.of(10));
    Assertions.assertEquals(0, result.namespaces().size());
    Assertions.assertNull(result.nextPageToken());
  }

  @Test
  void testPaginateTablesFirstPage() {
    Namespace ns = Namespace.of("db");
    ListTablesResponse response =
        ListTablesResponse.builder()
            .add(TableIdentifier.of(ns, "t1"))
            .add(TableIdentifier.of(ns, "t2"))
            .add(TableIdentifier.of(ns, "t3"))
            .build();
    ListTablesResponse result =
        IcebergPaginationHelper.paginateTables(response, Optional.empty(), Optional.of(2));
    Assertions.assertEquals(2, result.identifiers().size());
    Assertions.assertNotNull(result.nextPageToken());
  }

  @Test
  void testPaginateTablesFullWalk() {
    Namespace ns = Namespace.of("db");
    ListTablesResponse response =
        ListTablesResponse.builder()
            .add(TableIdentifier.of(ns, "t1"))
            .add(TableIdentifier.of(ns, "t2"))
            .add(TableIdentifier.of(ns, "t3"))
            .add(TableIdentifier.of(ns, "t4"))
            .add(TableIdentifier.of(ns, "t5"))
            .build();

    // Walk all pages with pageSize=2
    ListTablesResponse page1 =
        IcebergPaginationHelper.paginateTables(response, Optional.empty(), Optional.of(2));
    Assertions.assertEquals(2, page1.identifiers().size());
    Assertions.assertNotNull(page1.nextPageToken());

    ListTablesResponse page2 =
        IcebergPaginationHelper.paginateTables(
            response, Optional.ofNullable(page1.nextPageToken()), Optional.of(2));
    Assertions.assertEquals(2, page2.identifiers().size());
    Assertions.assertNotNull(page2.nextPageToken());

    ListTablesResponse page3 =
        IcebergPaginationHelper.paginateTables(
            response, Optional.ofNullable(page2.nextPageToken()), Optional.of(2));
    Assertions.assertEquals(1, page3.identifiers().size());
    Assertions.assertNull(page3.nextPageToken());

    // Verify all 5 items were returned
    int totalItems =
        page1.identifiers().size() + page2.identifiers().size() + page3.identifiers().size();
    Assertions.assertEquals(5, totalItems);
  }

  @Test
  void testPaginateTablesExactPageSize() {
    Namespace ns = Namespace.of("db");
    ListTablesResponse response =
        ListTablesResponse.builder()
            .add(TableIdentifier.of(ns, "t1"))
            .add(TableIdentifier.of(ns, "t2"))
            .build();
    // pageSize equals total items
    ListTablesResponse result =
        IcebergPaginationHelper.paginateTables(response, Optional.empty(), Optional.of(2));
    Assertions.assertEquals(2, result.identifiers().size());
    Assertions.assertNull(result.nextPageToken());
  }
}
