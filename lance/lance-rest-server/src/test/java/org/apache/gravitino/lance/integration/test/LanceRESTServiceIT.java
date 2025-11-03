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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.lancedb.lance.namespace.client.apache.ApiClient;
import com.lancedb.lance.namespace.client.apache.ApiException;
import com.lancedb.lance.namespace.client.apache.api.TableApi;
import com.lancedb.lance.namespace.model.CreateEmptyTableRequest;
import com.lancedb.lance.namespace.model.CreateEmptyTableResponse;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DeregisterTableRequest;
import com.lancedb.lance.namespace.model.DeregisterTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.JsonArrowField;
import com.lancedb.lance.namespace.model.RegisterTableRequest;
import com.lancedb.lance.namespace.model.RegisterTableRequest.ModeEnum;
import com.lancedb.lance.namespace.model.RegisterTableResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

@Tag("gravitino-docker-test")
@EnabledIf(value = "isDeploy", disabledReason = "Only run when deploy is true")
public class LanceRESTServiceIT extends BaseIT {
  public static final String METALAKE_NAME = GravitinoITUtils.genRandomName("lance_reset_metalake");
  public static final String CATALOG_NAME = GravitinoITUtils.genRandomName("lance_rest_catalog");
  public static final String SCHEMA_NAME = GravitinoITUtils.genRandomName("lance_rest_schema");
  private static final String DEFAULT_LANCE_REST_URL = "http://localhost:9101/lance";

  protected GravitinoMetalake metalake;
  protected Catalog catalog;
  private String tempDirectory;
  private TableApi tableApi;

  @BeforeAll
  public void setup() throws Exception {
    createMetalake();
    createCatalog();
    createSchema();

    // Create a temp directory for test use
    Path tempDir = Files.createTempDirectory("myTempDir");
    tempDirectory = tempDir.toString();
    File file = new File(tempDirectory);
    file.deleteOnExit();

    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(DEFAULT_LANCE_REST_URL);
    tableApi = new TableApi(apiClient);
  }

  protected void rewriteGravitinoServerConfig() throws IOException {
    customConfigs.put("gravitino.lance-rest.gravitino.metalake-name", METALAKE_NAME);
    super.rewriteGravitinoServerConfig();
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
    String delimiter = ".";
    String ids = Joiner.on(delimiter).join(CATALOG_NAME, SCHEMA_NAME, "empty_table");
    CreateEmptyTableRequest request = new CreateEmptyTableRequest();
    String location = tempDirectory + "/" + "empty_table/";
    request.setLocation(location);
    request.setProperties(ImmutableMap.of());

    CreateEmptyTableResponse response = tableApi.createEmptyTable(ids, request, delimiter);
    Assertions.assertNotNull(response);

    DescribeTableRequest describeTableRequest = new DescribeTableRequest();

    DescribeTableResponse loadTable = tableApi.describeTable(ids, describeTableRequest, delimiter);
    Assertions.assertNotNull(loadTable);
    Assertions.assertEquals(location, loadTable.getLocation());
  }

  @Test
  void testCreateTable() throws IOException, ApiException {
    String delimiter = ".";
    String ids = Joiner.on(delimiter).join(CATALOG_NAME, SCHEMA_NAME, "table");
    String location = tempDirectory + "/" + "table/";

    // TODO add more types here to verify it's okay.
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("value", new ArrowType.Utf8())));
    byte[] body = ArrowUtils.generateIpcStream(schema);

    CreateTableResponse response =
        tableApi.createTable(ids, body, delimiter, "create", location, "{}", ImmutableMap.of());
    Assertions.assertNotNull(response);
    Assertions.assertEquals(location, response.getLocation());

    DescribeTableRequest describeTableRequest = new DescribeTableRequest();
    DescribeTableResponse loadTable = tableApi.describeTable(ids, describeTableRequest, delimiter);
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
    response =
        Assertions.assertDoesNotThrow(
            () ->
                tableApi.createTable(
                    ids, body, delimiter, "overwrite", newLocation, "{}", ImmutableMap.of()));
    Assertions.assertNotNull(response);
    Assertions.assertEquals(newLocation, response.getLocation());
    Assertions.assertTrue(new File(newLocation).exists());
    Assertions.assertFalse(new File(location).exists());

    // Check exist_ok mode
    response =
        Assertions.assertDoesNotThrow(
            () ->
                tableApi.createTable(
                    ids, body, delimiter, "exist_ok", location, "{}", ImmutableMap.of()));
    Assertions.assertNotNull(response);
    Assertions.assertEquals(newLocation, response.getLocation());
    Assertions.assertTrue(new File(newLocation).exists());
  }

  @Test
  void testRegisterTable() throws ApiException {
    String delimiter = ".";
    String ids = Joiner.on(delimiter).join(CATALOG_NAME, SCHEMA_NAME, "table_register");
    String location = tempDirectory + "/" + "register/";
    RegisterTableRequest registerTableRequest = new RegisterTableRequest();
    registerTableRequest.setLocation(location);
    registerTableRequest.setMode(ModeEnum.CREATE);

    RegisterTableResponse response =
        tableApi.registerTable(ids, registerTableRequest, delimiter, ImmutableMap.of());
    Assertions.assertNotNull(response);

    DescribeTableRequest describeTableRequest = new DescribeTableRequest();
    DescribeTableResponse loadTable = tableApi.describeTable(ids, describeTableRequest, delimiter);
    Assertions.assertNotNull(loadTable);
    Assertions.assertEquals(location, loadTable.getLocation());

    // Test register again with OVERWRITE mode
    String newLocation = tempDirectory + "/" + "register_new/";
    registerTableRequest.setMode(ModeEnum.OVERWRITE);
    registerTableRequest.setLocation(newLocation);
    response =
        Assertions.assertDoesNotThrow(
            () -> tableApi.registerTable(ids, registerTableRequest, delimiter, ImmutableMap.of()));
    Assertions.assertNotNull(response);
    Assertions.assertEquals(newLocation, response.getLocation());

    // Test deregister table
    DeregisterTableRequest deregisterTableRequest = new DeregisterTableRequest();
    DeregisterTableResponse deregisterTableResponse =
        tableApi.deregisterTable(ids, deregisterTableRequest, delimiter);
    Assertions.assertNotNull(deregisterTableResponse);
    Assertions.assertEquals(newLocation, deregisterTableResponse.getLocation());
  }
}
