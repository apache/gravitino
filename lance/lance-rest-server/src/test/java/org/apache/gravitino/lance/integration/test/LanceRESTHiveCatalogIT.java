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
package org.apache.gravitino.lance.integration.test;

import static org.apache.gravitino.lance.common.config.LanceConfig.HIVE_CLIENT_POOL_SIZE;
import static org.apache.gravitino.lance.common.config.LanceConfig.HIVE_METASTORE_URIS;
import static org.apache.gravitino.lance.common.config.LanceConfig.HIVE_WAREHOUSE;
import static org.apache.gravitino.lance.common.config.LanceConfig.LANCE_CONFIG_PREFIX;
import static org.apache.gravitino.lance.common.config.LanceConfig.NAMESPACE_BACKEND;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.client.apache.ApiClient;
import org.lance.namespace.client.apache.api.TableApi;
import org.lance.namespace.errors.ErrorCode;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.RegisterTableRequest;
import org.lance.namespace.model.TableExistsRequest;

/**
 * Integration test for the Lance REST server backed by a Hive Metastore.
 *
 * <p>This test launches the embedded Gravitino server with the Lance REST aux-service configured to
 * use the {@code hive} namespace backend, pointed at a live Hive Metastore container. Unlike the
 * Gravitino backend (3-level catalog/schema/table), the Hive backend uses the 2-level {@code
 * db.table} model: a namespace is a single Hive database and a table is {@code db.table}.
 *
 * <p>Requires Docker; gated by the {@code gravitino-docker-test} tag.
 */
@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class LanceRESTHiveCatalogIT extends BaseIT {

  private static final ContainerSuite CONTAINER_SUITE = ContainerSuite.getInstance();
  private static final String DELIMITER = ".";

  private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private LanceNamespace ns;
  private String warehouse;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    // Start the Hive Metastore container before launching the Gravitino server so the
    // metastore URI can be wired into the Lance REST aux-service config.
    CONTAINER_SUITE.startHiveContainer();
    String hiveIp = CONTAINER_SUITE.getHiveContainer().getContainerIpAddress();
    String metastoreUris =
        String.format("thrift://%s:%d", hiveIp, HiveContainer.HIVE_METASTORE_PORT);
    this.warehouse =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse", hiveIp, HiveContainer.HDFS_DEFAULTFS_PORT);

    // Select and configure the Hive namespace backend for the Lance REST aux-service.
    Map<String, String> hiveConfigs = new HashMap<>();
    hiveConfigs.put(LANCE_CONFIG_PREFIX + NAMESPACE_BACKEND.getKey(), "hive");
    hiveConfigs.put(LANCE_CONFIG_PREFIX + HIVE_METASTORE_URIS.getKey(), metastoreUris);
    hiveConfigs.put(LANCE_CONFIG_PREFIX + HIVE_WAREHOUSE.getKey(), warehouse);
    hiveConfigs.put(LANCE_CONFIG_PREFIX + HIVE_CLIENT_POOL_SIZE.getKey(), "2");
    registerCustomConfigs(hiveConfigs);

    super.ignoreLanceAuxRestService = false;
    super.startIntegrationTest();

    Map<String, String> props = Maps.newHashMap();
    props.put("uri", getLanceRestServiceUrl());
    props.put("delimiter", DELIMITER);
    this.ns = LanceNamespace.connect("rest", props, allocator);
  }

  @AfterAll
  public void clean() throws Exception {
    Exception failure = null;
    try {
      allocator.close();
    } catch (Exception e) {
      failure = e;
    }
    try {
      super.stopIntegrationTest();
    } catch (Exception e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  @Test
  public void testNamespaceLifecycle() {
    String db = GravitinoITUtils.genRandomName("lance_hive_ns").toLowerCase();

    // Create the namespace (Hive database).
    CreateNamespaceRequest createReq = new CreateNamespaceRequest();
    createReq.addIdItem(db);
    createReq.setProperties(ImmutableMap.of("database.description", "lance hive it"));
    CreateNamespaceResponse createResp = ns.createNamespace(createReq);
    Assertions.assertNotNull(createResp);

    // It should now exist.
    NamespaceExistsRequest existsReq = new NamespaceExistsRequest();
    existsReq.addIdItem(db);
    Assertions.assertDoesNotThrow(() -> ns.namespaceExists(existsReq));

    // Describe should return the database properties (including the location).
    DescribeNamespaceRequest describeReq = new DescribeNamespaceRequest();
    describeReq.addIdItem(db);
    DescribeNamespaceResponse describeResp = ns.describeNamespace(describeReq);
    Assertions.assertNotNull(describeResp.getProperties());
    Assertions.assertTrue(describeResp.getProperties().containsKey("database.location-uri"));

    // The root namespace listing should include the new database.
    ListNamespacesResponse listResp = ns.listNamespaces(new ListNamespacesRequest());
    Assertions.assertTrue(listResp.getNamespaces().contains(db));

    // Drop the (empty) namespace.
    DropNamespaceRequest dropReq = new DropNamespaceRequest();
    dropReq.addIdItem(db);
    Assertions.assertDoesNotThrow(() -> ns.dropNamespace(dropReq));

    // Describing it again should fail.
    DescribeNamespaceRequest describeMissing = new DescribeNamespaceRequest();
    describeMissing.addIdItem(db);
    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class, () -> ns.describeNamespace(describeMissing));
    assertLanceErrorCode(ex, ErrorCode.NAMESPACE_NOT_FOUND);
  }

  @Test
  public void testCreateNamespaceModes() {
    String db = GravitinoITUtils.genRandomName("lance_hive_modes").toLowerCase();
    CreateNamespaceRequest createReq = new CreateNamespaceRequest();
    createReq.addIdItem(db);
    ns.createNamespace(createReq);

    // create again with default (create) mode should fail.
    RuntimeException ex =
        Assertions.assertThrows(RuntimeException.class, () -> ns.createNamespace(createReq));
    assertLanceErrorCode(ex, ErrorCode.NAMESPACE_ALREADY_EXISTS);

    // exist_ok mode should succeed.
    createReq.setMode("exist_ok");
    Assertions.assertDoesNotThrow(() -> ns.createNamespace(createReq));

    // overwrite mode should succeed (recreates the database).
    createReq.setMode("overwrite");
    Assertions.assertDoesNotThrow(() -> ns.createNamespace(createReq));

    DropNamespaceRequest dropReq = new DropNamespaceRequest();
    dropReq.addIdItem(db);
    ns.dropNamespace(dropReq);
  }

  @Test
  public void testDropNonEmptyNamespaceRestrictAndCascadeRejected() {
    String db = GravitinoITUtils.genRandomName("lance_hive_nonempty").toLowerCase();
    createDatabase(db);
    declareTable(db, "t1", tableLocation(db, "t1"));

    // RESTRICT (default) on a non-empty database should fail.
    DropNamespaceRequest restrictReq = new DropNamespaceRequest();
    restrictReq.addIdItem(db);
    RuntimeException restrictEx =
        Assertions.assertThrows(RuntimeException.class, () -> ns.dropNamespace(restrictReq));
    assertLanceErrorCode(restrictEx, ErrorCode.INVALID_INPUT);

    // CASCADE is not supported by the Hive backend.
    DropNamespaceRequest cascadeReq = new DropNamespaceRequest();
    cascadeReq.addIdItem(db);
    cascadeReq.setBehavior("cascade");
    RuntimeException cascadeEx =
        Assertions.assertThrows(RuntimeException.class, () -> ns.dropNamespace(cascadeReq));
    assertLanceErrorCode(cascadeEx, ErrorCode.INVALID_INPUT);

    // Clean up: drop the table, then the now-empty database.
    DropTableRequest dropTable = new DropTableRequest();
    dropTable.setId(List.of(db, "t1"));
    ns.dropTable(dropTable);
    DropNamespaceRequest dropDb = new DropNamespaceRequest();
    dropDb.addIdItem(db);
    ns.dropNamespace(dropDb);
  }

  @Test
  public void testDeclareDescribeAndListTables() {
    String db = GravitinoITUtils.genRandomName("lance_hive_tables").toLowerCase();
    createDatabase(db);

    String location = tableLocation(db, "declared");
    DeclareTableResponse declareResp = declareTable(db, "declared", location);
    Assertions.assertEquals(location, declareResp.getLocation());
    Assertions.assertEquals(Boolean.FALSE, declareResp.getManagedVersioning());

    // describeTable must request loadDetailedMetadata=false; the Hive backend rejects detailed
    // metadata because the metastore stores Lance tables as pointers without column schema.
    DescribeTableResponse describeResp = describeTable(db, "declared");
    Assertions.assertEquals(location, describeResp.getLocation());
    Assertions.assertEquals(Boolean.FALSE, describeResp.getManagedVersioning());
    Assertions.assertEquals("lance", describeResp.getProperties().get("table_type"));

    // tableExists should pass for the declared table.
    TableExistsRequest existsReq = new TableExistsRequest();
    existsReq.setId(List.of(db, "declared"));
    Assertions.assertDoesNotThrow(() -> ns.tableExists(existsReq));

    // listTables should return the declared lance table.
    ListTablesRequest listReq = new ListTablesRequest();
    listReq.setId(List.of(db));
    ListTablesResponse listResp = ns.listTables(listReq);
    Assertions.assertEquals(Sets.newHashSet("declared"), listResp.getTables());

    cleanupTable(db, "declared");
    dropDatabase(db);
  }

  @Test
  public void testDescribeTableDetailedMetadataRejected() {
    String db = GravitinoITUtils.genRandomName("lance_hive_detailed").toLowerCase();
    createDatabase(db);
    declareTable(db, "detailed", tableLocation(db, "detailed"));

    DescribeTableRequest req = new DescribeTableRequest();
    req.setId(List.of(db, "detailed"));
    req.setLoadDetailedMetadata(true);
    RuntimeException ex =
        Assertions.assertThrows(RuntimeException.class, () -> ns.describeTable(req));
    assertLanceErrorCode(ex, ErrorCode.INVALID_INPUT);

    cleanupTable(db, "detailed");
    dropDatabase(db);
  }

  @Test
  public void testDeclareAndDeregisterTable() {
    String db = GravitinoITUtils.genRandomName("lance_hive_register").toLowerCase();
    createDatabase(db);

    // The Hive backend creates tables through declareTable (metastore pointer); createTable and
    // registerTable are not supported by the underlying lance-namespace-hive2 implementation.
    String location = tableLocation(db, "declared");
    declareTable(db, "declared", location);
    Assertions.assertEquals(location, describeTable(db, "declared").getLocation());

    // Deregister keeps no data (external table) and removes the metastore entry.
    DeregisterTableRequest deregisterReq = new DeregisterTableRequest();
    deregisterReq.setId(List.of(db, "declared"));
    DeregisterTableResponse deregisterResp = ns.deregisterTable(deregisterReq);
    Assertions.assertEquals(location, deregisterResp.getLocation());

    // Describing the deregistered table should fail.
    DescribeTableRequest describeReq = new DescribeTableRequest();
    describeReq.setId(List.of(db, "declared"));
    describeReq.setLoadDetailedMetadata(false);
    RuntimeException ex =
        Assertions.assertThrows(RuntimeException.class, () -> ns.describeTable(describeReq));
    assertLanceErrorCode(ex, ErrorCode.TABLE_NOT_FOUND);

    dropDatabase(db);
  }

  @Test
  public void testRegisterTableUnsupported() {
    String db = GravitinoITUtils.genRandomName("lance_hive_register_unsupported").toLowerCase();
    createDatabase(db);

    RegisterTableRequest registerReq = new RegisterTableRequest();
    registerReq.setId(List.of(db, "registered"));
    registerReq.setLocation(tableLocation(db, "registered"));
    registerReq.setMode("create");
    // registerTable is not supported by the Hive backend.
    Assertions.assertThrows(RuntimeException.class, () -> ns.registerTable(registerReq));

    dropDatabase(db);
  }

  @Test
  public void testDropTable() {
    String db = GravitinoITUtils.genRandomName("lance_hive_drop").toLowerCase();
    createDatabase(db);
    declareTable(db, "to_drop", tableLocation(db, "to_drop"));

    DropTableRequest dropReq = new DropTableRequest();
    dropReq.setId(List.of(db, "to_drop"));
    Assertions.assertDoesNotThrow(() -> ns.dropTable(dropReq));

    // Dropping again should fail with TABLE_NOT_FOUND.
    RuntimeException ex =
        Assertions.assertThrows(RuntimeException.class, () -> ns.dropTable(dropReq));
    assertLanceErrorCode(ex, ErrorCode.TABLE_NOT_FOUND);

    dropDatabase(db);
  }

  @Test
  public void testTableNotFound() {
    String db = GravitinoITUtils.genRandomName("lance_hive_missing").toLowerCase();
    createDatabase(db);

    TableExistsRequest existsReq = new TableExistsRequest();
    existsReq.setId(List.of(db, "nope"));
    RuntimeException ex =
        Assertions.assertThrows(RuntimeException.class, () -> ns.tableExists(existsReq));
    assertLanceErrorCode(ex, ErrorCode.TABLE_NOT_FOUND);

    dropDatabase(db);
  }

  @Test
  public void testAlterTableUnsupported() {
    String db = GravitinoITUtils.genRandomName("lance_hive_alter").toLowerCase();
    createDatabase(db);
    declareTable(db, "altered", tableLocation(db, "altered"));

    AlterTableDropColumnsRequest dropColumns = new AlterTableDropColumnsRequest();
    dropColumns.setId(List.of(db, "altered"));
    dropColumns.setColumns(List.of("value"));

    // Column alteration is not supported by the Hive backend; the call must fail. The exact
    // message is not asserted because it is surfaced through the REST client as an HTTP error.
    TableApi tableApi = createTableApi();
    Assertions.assertThrows(
        Exception.class,
        () ->
            tableApi.alterTableDropColumns(
                String.join(DELIMITER, db, "altered"), dropColumns, DELIMITER));

    cleanupTable(db, "altered");
    dropDatabase(db);
  }

  private void createDatabase(String db) {
    CreateNamespaceRequest req = new CreateNamespaceRequest();
    req.addIdItem(db);
    ns.createNamespace(req);
  }

  private void dropDatabase(String db) {
    DropNamespaceRequest req = new DropNamespaceRequest();
    req.addIdItem(db);
    ns.dropNamespace(req);
  }

  private DeclareTableResponse declareTable(String db, String table, String location) {
    DeclareTableRequest req = new DeclareTableRequest();
    req.setId(List.of(db, table));
    req.setLocation(location);
    return ns.declareTable(req);
  }

  private DescribeTableResponse describeTable(String db, String table) {
    DescribeTableRequest req = new DescribeTableRequest();
    req.setId(List.of(db, table));
    req.setLoadDetailedMetadata(false);
    return ns.describeTable(req);
  }

  private void cleanupTable(String db, String table) {
    DropTableRequest req = new DropTableRequest();
    req.setId(List.of(db, table));
    Assertions.assertDoesNotThrow(() -> ns.dropTable(req));
  }

  private String tableLocation(String db, String table) {
    return String.format("%s/%s.db/%s", warehouse, db, table);
  }

  private TableApi createTableApi() {
    ApiClient apiClient = new ApiClient().setBasePath(getLanceRestServiceUrl());
    return new TableApi(apiClient);
  }

  private String getLanceRestServiceUrl() {
    return String.format("http://%s:%d/lance", "localhost", getLanceRESTServerPort());
  }

  private static void assertLanceErrorCode(RuntimeException exception, ErrorCode expected) {
    Assertions.assertInstanceOf(LanceNamespaceException.class, exception);
    Assertions.assertEquals(expected.getCode(), ((LanceNamespaceException) exception).getCode());
  }
}
