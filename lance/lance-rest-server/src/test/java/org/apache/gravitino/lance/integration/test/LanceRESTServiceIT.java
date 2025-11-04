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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.client.apache.ApiClient;
import com.lancedb.lance.namespace.client.apache.ApiException;
import com.lancedb.lance.namespace.model.CreateEmptyTableRequest;
import com.lancedb.lance.namespace.model.CreateEmptyTableResponse;
import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DeregisterTableRequest;
import com.lancedb.lance.namespace.model.DeregisterTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.JsonArrowField;
import com.lancedb.lance.namespace.model.ListTablesRequest;
import com.lancedb.lance.namespace.model.RegisterTableRequest;
import com.lancedb.lance.namespace.model.RegisterTableRequest.ModeEnum;
import com.lancedb.lance.namespace.model.RegisterTableResponse;
import com.lancedb.lance.namespace.rest.RestNamespace;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.lance.common.utils.ArrowUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.base.Joiner;

public class LanceRESTServiceIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(LanceRESTServiceIT.class);

  public static final String METALAKE_NAME = GravitinoITUtils.genRandomName("lance_reset_metalake");
  public static final String CATALOG_NAME = GravitinoITUtils.genRandomName("lance_rest_catalog");
  public static final String SCHEMA_NAME = GravitinoITUtils.genRandomName("lance_rest_schema");
  private static final String DEFAULT_LANCE_REST_URL = "http://localhost:9101/lance";

  protected GravitinoMetalake metalake;
  protected Catalog catalog;
  private String tempDirectory;
  private RestNamespace restNameSpace;

  @BeforeAll
  public void setup() throws Exception {
    startIntegrationTest();

    createMetalake();
    createCatalog();
    createSchema();

    // Create a temp directory for test use
    Path tempDir = Files.createTempDirectory("myTempDir");
    tempDirectory = tempDir.toString();
    File file = new File(tempDirectory);
    file.deleteOnExit();

    ApiClient apiClient = new ApiClient();
    String uri = DEFAULT_LANCE_REST_URL;
    if (serverConfig.getAllConfig().containsKey("gravitino.lance-rest.httpPort")) {
      int port = Integer.parseInt(serverConfig.getAllConfig().get("gravitino.lance-rest.httpPort"));
      uri = "http://localhost:" + port + "/lance";
      LOG.info("Lance REST HTTP Port: {}", port);
    }
    apiClient.setBasePath(uri);

    restNameSpace = new RestNamespace();
    Map<String, String> configs = ImmutableMap.of("delimiter", ".", "uri", uri);

    restNameSpace.initialize(configs, new RootAllocator());
  }

  public void startIntegrationTest() throws Exception {
    this.ignoreAuxRestService = false;
    customConfigs.put("gravitino.lance-rest.gravitino.metalake-name", METALAKE_NAME);
    super.startIntegrationTest();
  }

  private void createMetalake() {
    client.createMetalake(METALAKE_NAME, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(METALAKE_NAME);
    Assertions.assertEquals(METALAKE_NAME, loadMetalake.name());
    metalake = loadMetalake;
  }

  private void createCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    metalake.createCatalog(
        CATALOG_NAME, Catalog.Type.RELATIONAL, "generic-lakehouse", "comment", properties);
    catalog = metalake.loadCatalog(CATALOG_NAME);
  }

  protected void createSchema() {
    Map<String, String> schemaProperties = Maps.newHashMap();
    String comment = "comment";
    catalog.asSchemas().createSchema(SCHEMA_NAME, comment, schemaProperties);
    catalog.asSchemas().loadSchema(SCHEMA_NAME);
  }

  @Test
  void testCreateEmptyTable() throws ApiException {
    CreateEmptyTableRequest request = new CreateEmptyTableRequest();
    String location = tempDirectory + "/" + "empty_table/";
    request.setLocation(location);
    request.setProperties(ImmutableMap.of());
    request.setId(List.of(CATALOG_NAME, SCHEMA_NAME, "empty_table"));

    CreateEmptyTableResponse response = restNameSpace.createEmptyTable(request);
    Assertions.assertNotNull(response);

    DescribeTableRequest describeTableRequest = new DescribeTableRequest();
    describeTableRequest.setId(List.of(CATALOG_NAME, SCHEMA_NAME, "empty_table"));

    DescribeTableResponse loadTable = restNameSpace.describeTable(describeTableRequest);
    Assertions.assertNotNull(loadTable);
    Assertions.assertEquals(location, loadTable.getLocation());

    // Try to create the same table again should fail
    LanceNamespaceException exception =
        Assertions.assertThrows(
            LanceNamespaceException.class,
            () -> {
              restNameSpace.createEmptyTable(request);
            });
    Assertions.assertTrue(exception.getMessage().contains("Table already exists"));
    Assertions.assertEquals(409, exception.getCode());
  }

  @Test
  void testCreateTable() throws IOException, ApiException {
    String location = tempDirectory + "/" + "table/";
    List<String> ids = List.of(CATALOG_NAME, SCHEMA_NAME, "table");
    // TODO add more types here to verify it's okay.
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("value", new ArrowType.Utf8())));
    byte[] body = ArrowUtils.generateIpcStream(schema);

    CreateTableRequest request = new CreateTableRequest();
    request.setId(ids);
    request.setLocation(location);
    CreateTableResponse response = restNameSpace.createTable(request, body);
    Assertions.assertNotNull(response);
    Assertions.assertEquals(location, response.getLocation());

    DescribeTableRequest describeTableRequest = new DescribeTableRequest();
    describeTableRequest.setId(ids);
    DescribeTableResponse loadTable = restNameSpace.describeTable(describeTableRequest);
    Assertions.assertNotNull(loadTable);
    Assertions.assertEquals(location, loadTable.getLocation());

    List<JsonArrowField> jsonArrowFields = loadTable.getSchema().getFields();
    for (int i = 0; i < jsonArrowFields.size(); i++) {
      JsonArrowField jsonArrowField = jsonArrowFields.get(i);
      Field originalField = schema.getFields().get(i);
      Assertions.assertEquals(originalField.getName(), jsonArrowField.getName());

      if (i == 0) {
        Assertions.assertEquals("int32", jsonArrowField.getType().getType());
      } else if (i == 1) {
        Assertions.assertEquals("utf8", jsonArrowField.getType().getType());
      }
    }
    // Check the location exists
    Assertions.assertTrue(new File(location).exists());

    // Check overwrite mode
    String newLocation = tempDirectory + "/" + "table_new/";
    request.setLocation(newLocation);
    request.setMode(CreateTableRequest.ModeEnum.OVERWRITE);

    response = Assertions.assertDoesNotThrow(() -> restNameSpace.createTable(request, body));

    Assertions.assertNotNull(response);
    Assertions.assertEquals(newLocation, response.getLocation());
    Assertions.assertTrue(new File(newLocation).exists());
    Assertions.assertFalse(new File(location).exists());

    // Check exist_ok mode
    request.setMode(CreateTableRequest.ModeEnum.EXIST_OK);
    response = Assertions.assertDoesNotThrow(() -> restNameSpace.createTable(request, body));

    Assertions.assertNotNull(response);
    Assertions.assertEquals(newLocation, response.getLocation());
    Assertions.assertTrue(new File(newLocation).exists());

    // Create table again without overwrite or exist_ok should fail
    request.setMode(CreateTableRequest.ModeEnum.CREATE);
    LanceNamespaceException exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> restNameSpace.createTable(request, body));
    Assertions.assertTrue(exception.getMessage().contains("already exists"));
    Assertions.assertEquals(409, exception.getCode());

    // Create table with invalid schema should fail
    byte[] invalidBody = "invalid schema".getBytes(Charset.defaultCharset());
    CreateTableRequest invalidRequest = new CreateTableRequest();
    invalidRequest.setId(List.of(CATALOG_NAME, SCHEMA_NAME, "invalid_table"));
    invalidRequest.setLocation(tempDirectory + "/" + "invalid_table/");
    LanceNamespaceException apiException =
        Assertions.assertThrows(
            LanceNamespaceException.class,
            () -> restNameSpace.createTable(invalidRequest, invalidBody));
    Assertions.assertTrue(apiException.getMessage().contains("Failed to parse Arrow IPC stream"));
    Assertions.assertEquals(400, apiException.getCode());

    // Create table with wrong ids should fail
    CreateTableRequest wrongIdRequest = new CreateTableRequest();
    wrongIdRequest.setId(List.of(CATALOG_NAME, "wrong_schema")); // This is a schema NOT a talbe.
    wrongIdRequest.setLocation(tempDirectory + "/" + "wrong_id_table/");
    LanceNamespaceException wrongIdException =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> restNameSpace.createTable(wrongIdRequest, body));
    Assertions.assertTrue(wrongIdException.getMessage().contains("Expected at 3-level namespace"));
    Assertions.assertEquals(400, wrongIdException.getCode());

    // Now test list tables
    ListTablesRequest listRequest = new ListTablesRequest();
    listRequest.setId(List.of(CATALOG_NAME, SCHEMA_NAME));
    var listResponse = restNameSpace.listTables(listRequest);
    Set<String> stringSet = listResponse.getTables();
    Assertions.assertEquals(1, stringSet.size());
    Assertions.assertTrue(stringSet.contains(Joiner.on(".").join(ids)));
  }

  @Test
  void testRegisterTable() {
    String location = tempDirectory + "/" + "register/";
    List<String> ids = List.of(CATALOG_NAME, SCHEMA_NAME, "table_register");
    RegisterTableRequest registerTableRequest = new RegisterTableRequest();
    registerTableRequest.setLocation(location);
    registerTableRequest.setMode(ModeEnum.CREATE);
    registerTableRequest.setId(ids);
    registerTableRequest.setProperties(ImmutableMap.of("key1", "value1"));

    RegisterTableResponse response = restNameSpace.registerTable(registerTableRequest);
    Assertions.assertNotNull(response);

    DescribeTableRequest describeTableRequest = new DescribeTableRequest();
    describeTableRequest.setId(ids);
    DescribeTableResponse loadTable = restNameSpace.describeTable(describeTableRequest);
    Assertions.assertNotNull(loadTable);
    Assertions.assertEquals(location, loadTable.getLocation());
    Assertions.assertTrue(loadTable.getProperties().containsKey("key1"));

    // Test register again with OVERWRITE mode
    String newLocation = tempDirectory + "/" + "register_new/";
    registerTableRequest.setMode(ModeEnum.OVERWRITE);
    registerTableRequest.setLocation(newLocation);
    response =
        Assertions.assertDoesNotThrow(() -> restNameSpace.registerTable(registerTableRequest));
    Assertions.assertNotNull(response);
    Assertions.assertEquals(newLocation, response.getLocation());

    // Test deregister table
    DeregisterTableRequest deregisterTableRequest = new DeregisterTableRequest();
    deregisterTableRequest.setId(ids);
    DeregisterTableResponse deregisterTableResponse =
        restNameSpace.deregisterTable(deregisterTableRequest);
    Assertions.assertNotNull(deregisterTableResponse);
    Assertions.assertEquals(newLocation, deregisterTableResponse.getLocation());
  }

  @Test
  void testDeregisterNonExistingTable() {
    List<String> ids = List.of(CATALOG_NAME, SCHEMA_NAME, "non_existing_table");
    DeregisterTableRequest deregisterTableRequest = new DeregisterTableRequest();
    deregisterTableRequest.setId(ids);

    LanceNamespaceException exception =
        Assertions.assertThrows(
            LanceNamespaceException.class,
            () -> restNameSpace.deregisterTable(deregisterTableRequest));
    Assertions.assertEquals(404, exception.getCode());
    Assertions.assertTrue(exception.getMessage().contains("does not exist"));

    // Try to create a table and then deregister table
    CreateEmptyTableRequest createEmptyTableRequest = new CreateEmptyTableRequest();
    String location = tempDirectory + "/" + "to_be_deregistered_table/";
    ids = List.of(CATALOG_NAME, SCHEMA_NAME, "to_be_deregistered_table");
    createEmptyTableRequest.setLocation(location);
    createEmptyTableRequest.setProperties(ImmutableMap.of());
    createEmptyTableRequest.setId(ids);
    CreateEmptyTableResponse response =
        Assertions.assertDoesNotThrow(
            () -> restNameSpace.createEmptyTable(createEmptyTableRequest));
    Assertions.assertNotNull(response);
    Assertions.assertEquals(location, response.getLocation());

    // Now try to deregister
    deregisterTableRequest.setId(ids);
    DeregisterTableResponse deregisterTableResponse =
        Assertions.assertDoesNotThrow(() -> restNameSpace.deregisterTable(deregisterTableRequest));
    Assertions.assertNotNull(deregisterTableResponse);
    Assertions.assertEquals(location, deregisterTableResponse.getLocation());
    Assertions.assertTrue(
        new File(location).exists(), "Data should still exist after deregistering the table.");

    // Now try to describe the table, should fail
    DescribeTableRequest describeTableRequest = new DescribeTableRequest();
    describeTableRequest.setId(ids);
    LanceNamespaceException lanceNamespaceException =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> restNameSpace.describeTable(describeTableRequest));
    Assertions.assertEquals(404, lanceNamespaceException.getCode());
  }
}
