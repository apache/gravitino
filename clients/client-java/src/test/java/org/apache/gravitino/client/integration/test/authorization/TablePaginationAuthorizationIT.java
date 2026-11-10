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
package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.pagination.Page;
import org.apache.gravitino.pagination.PageRequest;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Paged table listing against a running server with authorization enabled, over a Hive catalog
 * whose table list comes from the metastore.
 */
@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TablePaginationAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG = "paging_catalog";
  private static final String SCHEMA = "paging_schema";
  private static final String ROLE = "paging_role";
  private static final String EMPTY_USER = "paging_no_tables_user";
  private static final String EMPTY_ROLE = "paging_no_tables_role";
  private static final int TABLE_COUNT = 25;
  private static final Namespace NS = Namespace.of(SCHEMA);
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  /**
   * Tables the normal user may see. They are spread so that page boundaries fall on runs of hidden
   * tables, and the list ends with a hidden run after t23 is followed by t25.
   */
  private static final List<String> VISIBLE =
      ImmutableList.of("t01", "t04", "t05", "t06", "t11", "t12", "t17", "t20", "t23", "t25");

  private static GravitinoAdminClient emptyUserClient;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startHiveContainer();
    super.startIntegrationTest();
    String hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake
        .createCatalog(CATALOG, Catalog.Type.RELATIONAL, "hive", "comment", properties)
        .asSchemas()
        .createSchema(SCHEMA, "test", new HashMap<>());

    TableCatalog tables = metalake.loadCatalog(CATALOG).asTableCatalog();
    // Create in reverse order so the metastore's order is not the sorted order.
    for (int i = TABLE_COUNT; i >= 1; i--) {
      tables.createTable(NameIdentifier.of(SCHEMA, name(i)), columns(), "test", new HashMap<>());
    }

    metalake.createRole(ROLE, new HashMap<>(), new ArrayList<>());
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        ImmutableList.of(Privileges.UseCatalog.allow()));
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow()));
    for (String table : VISIBLE) {
      metalake.grantPrivilegesToRole(
          ROLE,
          MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, table), MetadataObject.Type.TABLE),
          ImmutableList.of(Privileges.SelectTable.allow()));
    }
    metalake.grantRolesToUser(ImmutableList.of(ROLE), NORMAL_USER);

    // A user who can use the catalog and schema but holds no privilege on any table.
    metalake.addUser(EMPTY_USER);
    metalake.createRole(EMPTY_ROLE, new HashMap<>(), new ArrayList<>());
    metalake.grantPrivilegesToRole(
        EMPTY_ROLE,
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        ImmutableList.of(Privileges.UseCatalog.allow()));
    metalake.grantPrivilegesToRole(
        EMPTY_ROLE,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow()));
    metalake.grantRolesToUser(ImmutableList.of(EMPTY_ROLE), EMPTY_USER);
    emptyUserClient = GravitinoAdminClient.builder(serverUri).withSimpleAuth(EMPTY_USER).build();
  }

  @AfterAll
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (emptyUserClient != null) {
      emptyUserClient.close();
    }
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.loadCatalog(CATALOG).asSchemas().dropSchema(SCHEMA, true);
    metalake.dropCatalog(CATALOG, true);
    super.stopIntegrationTest();
  }

  @Test
  @Order(1)
  public void testOwnerWalksEveryTableInOrder() {
    List<String> walked = walk(tableCatalog(client), 3, new ArrayList<>());
    List<String> expected =
        IntStream.rangeClosed(1, TABLE_COUNT)
            .mapToObj(TablePaginationAuthorizationIT::name)
            .collect(Collectors.toList());
    assertEquals(expected, walked);
  }

  @Test
  @Order(2)
  public void testPagesAreFullWhenHiddenTablesFallOnBoundaries() {
    List<Integer> pageSizes = new ArrayList<>();
    List<String> walked = walk(tableCatalog(normalUserClient), 3, pageSizes);
    assertEquals(VISIBLE, walked);
    assertEquals(ImmutableList.of(3, 3, 3, 1), pageSizes);

    // The unpaged list holds the same tables.
    Set<String> unpaged =
        Arrays.stream(tableCatalog(normalUserClient).listTables(NS))
            .map(NameIdentifier::name)
            .collect(Collectors.toSet());
    assertEquals(new HashSet<>(VISIBLE), unpaged);
  }

  @Test
  @Order(3)
  public void testLastVisibleTableBeforeHiddenRunEndsTheWalk() {
    // A page that ends at t23 must still return a token, because t25 is visible after the hidden
    // t24; the page after it holds t25 alone and no token.
    TableCatalog catalog = tableCatalog(normalUserClient);
    Page<NameIdentifier> page = catalog.listTables(NS, PageRequest.ofSize(9));
    assertEquals(VISIBLE.subList(0, 9), names(page));
    assertTrue(page.nextPageToken().isPresent());
    Page<NameIdentifier> last =
        catalog.listTables(NS, PageRequest.ofSize(9).withToken(page.nextPageToken().get()));
    assertEquals(ImmutableList.of("t25"), names(last));
    assertFalse(last.nextPageToken().isPresent());
  }

  @Test
  @Order(4)
  public void testUserWithNoVisibleTablesGetsEmptyPageWithoutToken() {
    Page<NameIdentifier> page = tableCatalog(emptyUserClient).listTables(NS, PageRequest.ofSize(5));
    assertTrue(page.items().isEmpty());
    assertFalse(page.nextPageToken().isPresent());
  }

  @Test
  @Order(5)
  public void testInvalidTokensAreRejected() {
    TableCatalog catalog = tableCatalog(normalUserClient);
    IllegalArgumentException malformed =
        assertThrows(
            IllegalArgumentException.class,
            () -> catalog.listTables(NS, PageRequest.ofSize(3).withToken("not-a-token")));
    assertTrue(malformed.getMessage().contains("pageToken"), malformed.getMessage());

    // A token issued for this schema is rejected for another schema.
    String token = catalog.listTables(NS, PageRequest.ofSize(3)).nextPageToken().get();
    client
        .loadMetalake(METALAKE)
        .loadCatalog(CATALOG)
        .asSchemas()
        .createSchema("paging_other", "test", new HashMap<>());
    try {
      IllegalArgumentException wrongList =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  tableCatalog(client)
                      .listTables(
                          Namespace.of("paging_other"), PageRequest.ofSize(3).withToken(token)));
      assertTrue(wrongList.getMessage().contains("pageToken"), wrongList.getMessage());
    } finally {
      client
          .loadMetalake(METALAKE)
          .loadCatalog(CATALOG)
          .asSchemas()
          .dropSchema("paging_other", true);
    }
  }

  @Test
  @Order(6)
  public void testCreatesAndDropsBetweenPages() {
    TableCatalog catalog = tableCatalog(client);
    List<String> walked = new ArrayList<>();

    Page<NameIdentifier> page = catalog.listTables(NS, PageRequest.ofSize(4));
    walked.addAll(names(page)); // t01..t04

    // Between page requests: drop a table already returned and one not yet reached, and create
    // one behind the cursor and one ahead of it.
    catalog.dropTable(NameIdentifier.of(SCHEMA, "t02"));
    catalog.dropTable(NameIdentifier.of(SCHEMA, "t10"));
    catalog.createTable(NameIdentifier.of(SCHEMA, "t03a"), columns(), "test", new HashMap<>());
    catalog.createTable(NameIdentifier.of(SCHEMA, "t15a"), columns(), "test", new HashMap<>());
    try {
      while (page.nextPageToken().isPresent()) {
        page = catalog.listTables(NS, PageRequest.ofSize(4).withToken(page.nextPageToken().get()));
        walked.addAll(names(page));
      }

      assertEquals(walked.size(), new HashSet<>(walked).size(), "no table twice: " + walked);
      for (int i = 1; i <= TABLE_COUNT; i++) {
        if (i != 10) {
          assertTrue(walked.contains(name(i)), "missing " + name(i) + ": " + walked);
        }
      }
      assertFalse(walked.contains("t10"), "dropped before the walk reached it");
      assertFalse(walked.contains("t03a"), "created behind the cursor");
      assertTrue(walked.contains("t15a"), "created ahead of the cursor");
      List<String> sorted = new ArrayList<>(walked);
      sorted.sort(String::compareTo);
      assertArrayEquals(sorted.toArray(), walked.toArray(), "walk order");
    } finally {
      catalog.dropTable(NameIdentifier.of(SCHEMA, "t03a"));
      catalog.dropTable(NameIdentifier.of(SCHEMA, "t15a"));
      catalog.createTable(NameIdentifier.of(SCHEMA, "t02"), columns(), "test", new HashMap<>());
      catalog.createTable(NameIdentifier.of(SCHEMA, "t10"), columns(), "test", new HashMap<>());
    }
  }

  private static List<String> walk(TableCatalog catalog, int pageSize, List<Integer> pageSizes) {
    List<String> walked = new ArrayList<>();
    Page<NameIdentifier> page = catalog.listTables(NS, PageRequest.ofSize(pageSize));
    while (true) {
      pageSizes.add(page.items().size());
      walked.addAll(names(page));
      if (!page.nextPageToken().isPresent()) {
        return walked;
      }
      assertEquals(pageSize, page.items().size(), "only the last page may be short");
      page =
          catalog.listTables(
              NS, PageRequest.ofSize(pageSize).withToken(page.nextPageToken().get()));
    }
  }

  private static TableCatalog tableCatalog(GravitinoAdminClient adminClient) {
    return adminClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
  }

  private static List<String> names(Page<NameIdentifier> page) {
    return page.items().stream().map(NameIdentifier::name).collect(Collectors.toList());
  }

  private static String name(int i) {
    return String.format("t%02d", i);
  }

  private static Column[] columns() {
    return new Column[] {Column.of("col1", Types.StringType.get())};
  }
}
