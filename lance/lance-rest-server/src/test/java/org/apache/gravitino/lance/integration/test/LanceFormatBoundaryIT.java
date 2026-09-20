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
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.lance.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.lance.common.utils.ArrowUtils;
import org.apache.gravitino.lance.common.utils.LanceConstants;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.client.apache.ApiClient;
import org.lance.namespace.client.apache.ApiException;
import org.lance.namespace.client.apache.api.TableApi;
import org.lance.namespace.errors.ErrorCode;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.RegisterTableRequest;
import org.lance.namespace.model.TableExistsRequest;

/** Integration coverage for the Lance/non-Lance table format boundary. */
public class LanceFormatBoundaryIT extends BaseIT {
  private static final String CATALOG_NAME =
      GravitinoITUtils.genRandomName("lance_boundary_catalog");
  private static final String SCHEMA_NAME = GravitinoITUtils.genRandomName("lance_boundary_schema");
  private static final String DELIMITER = ".";

  private GravitinoMetalake metalake;
  private Catalog catalog;
  private LanceNamespace namespace;
  private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private Path tempDir;

  /** Starts an embedded Gravitino server with the Lance auxiliary service. */
  @BeforeAll
  public void startIntegrationTest() throws Exception {
    ignoreLanceAuxRestService = false;
    super.startIntegrationTest();
    metalake =
        client.createMetalake(getLanceRESTServerMetalakeName(), "Lance format boundary IT", null);
    catalog =
        metalake.createCatalog(
            CATALOG_NAME,
            Catalog.Type.RELATIONAL,
            "lakehouse-generic",
            "Lance format boundary catalog",
            ImmutableMap.of());
    catalog.asSchemas().createSchema(SCHEMA_NAME, "Lance format boundary schema", null);
    namespace =
        LanceNamespace.connect(
            "rest",
            ImmutableMap.of("uri", getLanceRestServiceUrl(), "delimiter", DELIMITER),
            allocator);
    tempDir = Files.createTempDirectory("lance_format_boundary_it_");
  }

  /** Stops the test services and removes test metadata and temporary files. */
  @AfterAll
  public void clean() throws Exception {
    Exception failure = null;
    try {
      if (client != null) {
        client.dropMetalake(getLanceRESTServerMetalakeName(), true);
      }
    } catch (Exception e) {
      failure = e;
    }

    try {
      if (tempDir != null) {
        FileUtils.deleteDirectory(tempDir.toFile());
      }
    } catch (Exception e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }

    try {
      allocator.close();
    } catch (Exception e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
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

  /** Verifies that non-Lance metadata and data are preserved by all rejected Lance operations. */
  @Test
  public void testNonLanceTablesFailClosedWithoutDataLoss() throws IOException {
    String deltaTableName = "delta_boundary_table";
    String deltaLocation = tempDir.resolve(deltaTableName).toString();
    Path sentinel = Files.createDirectories(Path.of(deltaLocation)).resolve("sentinel");
    Files.writeString(sentinel, "must survive");
    NameIdentifier deltaIdentifier = NameIdentifier.of(SCHEMA_NAME, deltaTableName);
    catalog
        .asTableCatalog()
        .createTable(
            deltaIdentifier,
            new Column[] {Column.of("id", Types.IntegerType.get(), "id")},
            null,
            ImmutableMap.of(
                Table.PROPERTY_LOCATION,
                deltaLocation,
                Table.PROPERTY_TABLE_FORMAT,
                "delta",
                Table.PROPERTY_EXTERNAL,
                "true"));

    List<String> deltaIds = List.of(CATALOG_NAME, SCHEMA_NAME, deltaTableName);
    DescribeTableRequest describeRequest = new DescribeTableRequest();
    describeRequest.setId(deltaIds);
    LanceNamespaceException describeException =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> namespace.describeTable(describeRequest));
    assertLanceErrorCode(describeException, ErrorCode.INVALID_INPUT);

    Table deltaTable = catalog.asTableCatalog().loadTable(deltaIdentifier);
    String originalDeltaLocation = deltaTable.properties().get(Table.PROPERTY_LOCATION);
    Assertions.assertEquals("delta", deltaTable.properties().get(Table.PROPERTY_TABLE_FORMAT));

    TableCatalog tableCatalog = catalog.asTableCatalog();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableCatalog.createTable(
                deltaIdentifier, new Column[0], null, overwriteProperties(deltaLocation, false)));
    Assertions.assertTrue(Files.exists(sentinel));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableCatalog.createTable(
                deltaIdentifier, new Column[0], null, overwriteProperties(deltaLocation, true)));
    Assertions.assertTrue(Files.exists(sentinel));

    ApiException existOkException =
        Assertions.assertThrows(
            ApiException.class,
            () ->
                createTableApi()
                    .createTable(
                        String.join(DELIMITER, deltaIds),
                        arrowBody(),
                        DELIMITER,
                        "exist_ok",
                        null,
                        null,
                        Map.of(
                            LanceConstants.LANCE_TABLE_LOCATION_HEADER,
                            tempDir.resolve("rest_exist_ok").toString())));
    Assertions.assertEquals(400, existOkException.getCode());
    Assertions.assertTrue(Files.exists(sentinel));

    ApiException overwriteException =
        Assertions.assertThrows(
            ApiException.class,
            () ->
                createTableApi()
                    .createTable(
                        String.join(DELIMITER, deltaIds),
                        arrowBody(),
                        DELIMITER,
                        "overwrite",
                        null,
                        null,
                        Map.of(
                            LanceConstants.LANCE_TABLE_LOCATION_HEADER,
                            tempDir.resolve("rest_overwrite").toString())));
    Assertions.assertEquals(400, overwriteException.getCode());
    Assertions.assertTrue(Files.exists(sentinel));

    RegisterTableRequest registerRequest = new RegisterTableRequest();
    registerRequest.setId(deltaIds);
    registerRequest.setLocation(tempDir.resolve("rest_register_overwrite").toString());
    registerRequest.setMode("overwrite");
    LanceNamespaceException registerException =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> namespace.registerTable(registerRequest));
    assertLanceErrorCode(registerException, ErrorCode.INVALID_INPUT);
    Assertions.assertTrue(Files.exists(sentinel));

    ApiException createException =
        Assertions.assertThrows(
            ApiException.class,
            () ->
                createTableApi()
                    .createTable(
                        String.join(DELIMITER, deltaIds),
                        arrowBody(),
                        DELIMITER,
                        "create",
                        null,
                        null,
                        Map.of(
                            LanceConstants.LANCE_TABLE_LOCATION_HEADER,
                            tempDir.resolve("rest_create").toString())));
    Assertions.assertEquals(409, createException.getCode());
    Assertions.assertTrue(Files.exists(sentinel));

    LanceNamespaceException existsException =
        Assertions.assertThrows(
            LanceNamespaceException.class,
            () -> namespace.tableExists(tableExistsRequest(deltaIds)));
    assertLanceErrorCode(existsException, ErrorCode.TABLE_NOT_FOUND);
    Table remainingDeltaTable = tableCatalog.loadTable(deltaIdentifier);
    Assertions.assertEquals(
        "delta", remainingDeltaTable.properties().get(Table.PROPERTY_TABLE_FORMAT));
    Assertions.assertEquals(
        originalDeltaLocation, remainingDeltaTable.properties().get(Table.PROPERTY_LOCATION));

    LanceNamespaceException dropException =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> namespace.dropTable(dropTableRequest(deltaIds)));
    assertLanceErrorCode(dropException, ErrorCode.INVALID_INPUT);
    Assertions.assertTrue(Files.exists(sentinel));

    LanceNamespaceException deregisterException =
        Assertions.assertThrows(
            LanceNamespaceException.class,
            () -> namespace.deregisterTable(deregisterTableRequest(deltaIds)));
    assertLanceErrorCode(deregisterException, ErrorCode.INVALID_INPUT);
    Assertions.assertTrue(Files.exists(sentinel));

    AlterTableDropColumnsRequest alterRequest = new AlterTableDropColumnsRequest();
    alterRequest.setId(deltaIds);
    alterRequest.setColumns(List.of("id"));
    ApiException alterException =
        Assertions.assertThrows(
            ApiException.class,
            () ->
                createTableApi()
                    .alterTableDropColumns(
                        String.join(DELIMITER, deltaIds), alterRequest, DELIMITER));
    Assertions.assertEquals(400, alterException.getCode());
    Assertions.assertTrue(Files.exists(sentinel));

    String lanceTableName = "lance_boundary_table";
    List<String> lanceIds = List.of(CATALOG_NAME, SCHEMA_NAME, lanceTableName);
    DeclareTableRequest declareRequest = new DeclareTableRequest();
    declareRequest.setId(lanceIds);
    declareRequest.setLocation(tempDir.resolve(lanceTableName).toString());
    Assertions.assertDoesNotThrow(() -> namespace.declareTable(declareRequest));

    DescribeTableRequest lanceDescribeRequest = new DescribeTableRequest();
    lanceDescribeRequest.setId(lanceIds);
    Assertions.assertEquals(
        "lance",
        namespace
            .describeTable(lanceDescribeRequest)
            .getMetadata()
            .get(Table.PROPERTY_TABLE_FORMAT));
    Assertions.assertDoesNotThrow(() -> namespace.tableExists(tableExistsRequest(lanceIds)));
  }

  private Map<String, String> overwriteProperties(String location, boolean register) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(Table.PROPERTY_LOCATION, location);
    properties.put(Table.PROPERTY_TABLE_FORMAT, "lance");
    properties.put(Table.PROPERTY_EXTERNAL, "true");
    properties.put(LanceConstants.LANCE_CREATION_MODE, "OVERWRITE");
    if (register) {
      properties.put(LanceConstants.LANCE_TABLE_REGISTER, "true");
    }
    return properties;
  }

  private TableExistsRequest tableExistsRequest(List<String> ids) {
    TableExistsRequest request = new TableExistsRequest();
    request.setId(ids);
    return request;
  }

  private DropTableRequest dropTableRequest(List<String> ids) {
    DropTableRequest request = new DropTableRequest();
    request.setId(ids);
    return request;
  }

  private DeregisterTableRequest deregisterTableRequest(List<String> ids) {
    DeregisterTableRequest request = new DeregisterTableRequest();
    request.setId(ids);
    return request;
  }

  private TableApi createTableApi() {
    return new TableApi(new ApiClient().setBasePath(getLanceRestServiceUrl()));
  }

  private static byte[] arrowBody() throws IOException {
    return ArrowUtils.generateIpcStream(
        new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true)))));
  }

  private static void assertLanceErrorCode(
      RuntimeException exception, ErrorCode expectedErrorCode) {
    Assertions.assertInstanceOf(LanceNamespaceException.class, exception);
    Assertions.assertEquals(
        expectedErrorCode.getCode(), ((LanceNamespaceException) exception).getCode());
  }

  private String getLanceRestServiceUrl() {
    return String.format("http://%s:%d/lance", "localhost", getLanceRESTServerPort());
  }
}
