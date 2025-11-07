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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.Transaction;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.ipc.LanceScanner;
import com.lancedb.lance.ipc.ScanOptions;
import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.LanceNamespaces;
import com.lancedb.lance.namespace.client.apache.ApiException;
import com.lancedb.lance.namespace.model.CreateEmptyTableRequest;
import com.lancedb.lance.namespace.model.CreateEmptyTableResponse;
import com.lancedb.lance.namespace.model.CreateNamespaceRequest;
import com.lancedb.lance.namespace.model.CreateNamespaceResponse;
import com.lancedb.lance.namespace.model.CreateTableIndexRequest;
import com.lancedb.lance.namespace.model.CreateTableIndexRequest.IndexTypeEnum;
import com.lancedb.lance.namespace.model.CreateTableIndexRequest.MetricTypeEnum;
import com.lancedb.lance.namespace.model.CreateTableIndexResponse;
import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DeregisterTableRequest;
import com.lancedb.lance.namespace.model.DeregisterTableResponse;
import com.lancedb.lance.namespace.model.DescribeNamespaceRequest;
import com.lancedb.lance.namespace.model.DescribeNamespaceResponse;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.DropNamespaceRequest;
import com.lancedb.lance.namespace.model.DropNamespaceResponse;
import com.lancedb.lance.namespace.model.ErrorResponse;
import com.lancedb.lance.namespace.model.IndexContent;
import com.lancedb.lance.namespace.model.JsonArrowField;
import com.lancedb.lance.namespace.model.ListNamespacesRequest;
import com.lancedb.lance.namespace.model.ListNamespacesResponse;
import com.lancedb.lance.namespace.model.ListTableIndicesRequest;
import com.lancedb.lance.namespace.model.ListTableIndicesResponse;
import com.lancedb.lance.namespace.model.ListTablesRequest;
import com.lancedb.lance.namespace.model.NamespaceExistsRequest;
import com.lancedb.lance.namespace.model.RegisterTableRequest;
import com.lancedb.lance.namespace.model.RegisterTableRequest.ModeEnum;
import com.lancedb.lance.namespace.model.RegisterTableResponse;
import com.lancedb.lance.namespace.rest.RestNamespaceConfig;
import com.lancedb.lance.operation.Append;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.compress.utils.Lists;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.lance.common.utils.ArrowUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.base.Joiner;

public class LanceRESTServiceIT extends BaseIT {
  private static final String CATALOG_NAME = GravitinoITUtils.genRandomName("lance_rest_catalog");
  private static final String SCHEMA_NAME = GravitinoITUtils.genRandomName("lance_rest_schema");

  private GravitinoMetalake metalake;
  private Catalog catalog;
  private Map<String, String> properties =
      new HashMap<>() {
        {
          put("key1", "value1");
        }
      };
  private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private LanceNamespace ns;
  private Path tempDir;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.ignoreLanceAuxRestService = false;
    super.startIntegrationTest();
    this.metalake = createMetalake(getLanceRESTServerMetalakeName());

    HashMap<String, String> props = Maps.newHashMap();
    props.put(RestNamespaceConfig.URI, getLanceRestServiceUrl());
    props.put(RestNamespaceConfig.DELIMITER, RestNamespaceConfig.DELIMITER_DEFAULT);
    this.ns = LanceNamespaces.connect("rest", props, null, allocator);

    this.tempDir = Files.createTempDirectory("test_lance_rest_service_it_");
  }

  @AfterAll
  public void clean() {
    client.dropMetalake(getLanceRESTServerMetalakeName(), true);
    tempDir.toFile().deleteOnExit();
  }

  @AfterEach
  public void clearMetalake() {
    Arrays.stream(metalake.listCatalogs()).forEach(c -> metalake.dropCatalog(c, true));
  }

  @Test
  public void testListNamespaces() {
    Catalog catalog1 = createCatalog(GravitinoITUtils.genRandomName("lance_catalog_1"));
    Catalog catalog2 = createCatalog(GravitinoITUtils.genRandomName("lance_catalog_2"));
    Schema schema1 =
        catalog1
            .asSchemas()
            .createSchema("lance_schema_1", "schema for lance rest service tests", null);

    // test list catalogs via lance rest namespace client
    ListNamespacesRequest listNamespacesReq = new ListNamespacesRequest();
    ListNamespacesResponse listNamespacesResp = ns.listNamespaces(listNamespacesReq);

    Assertions.assertEquals(
        Sets.newHashSet(catalog1.name(), catalog2.name()), listNamespacesResp.getNamespaces());

    // test list schemas via lance rest namespace client
    listNamespacesReq.addIdItem(catalog1.name());
    listNamespacesResp = ns.listNamespaces(listNamespacesReq);

    Assertions.assertEquals(Sets.newHashSet(schema1.name()), listNamespacesResp.getNamespaces());
  }

  @Test
  public void testDescribeNamespace() {
    Catalog catalog = createCatalog(GravitinoITUtils.genRandomName("lance_catalog"));
    Map<String, String> schemaProps =
        new HashMap<>() {
          {
            put("schema_key1", "schema_value1");
          }
        };
    Schema schema = catalog.asSchemas().createSchema("lance_schema", null, schemaProps);

    // test describe catalog via lance rest namespace client
    DescribeNamespaceRequest describeNamespaceReq = new DescribeNamespaceRequest();
    describeNamespaceReq.addIdItem(catalog.name());
    DescribeNamespaceResponse describeNamespaceResp = ns.describeNamespace(describeNamespaceReq);

    Assertions.assertEquals(catalog.properties(), describeNamespaceResp.getProperties());

    // test describe schema via lance rest namespace client
    describeNamespaceReq.addIdItem(schema.name());
    describeNamespaceResp = ns.describeNamespace(describeNamespaceReq);

    Assertions.assertEquals(schema.properties(), describeNamespaceResp.getProperties());

    // test describe the root namespace
    DescribeNamespaceRequest rootDescNamespaceReq = new DescribeNamespaceRequest();
    LanceNamespaceException exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.describeNamespace(rootDescNamespaceReq));

    Assertions.assertEquals(400, exception.getCode());
    Assertions.assertTrue(exception.getErrorResponse().isPresent());
    Assertions.assertTrue(
        exception
            .getErrorResponse()
            .get()
            .getError()
            .contains("Expected at most 2-level and at least 1-level namespace"));
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(),
        exception.getErrorResponse().get().getType());

    // test describe a non-existent catalog namespace
    DescribeNamespaceRequest nonExistentCatalogReq = new DescribeNamespaceRequest();
    nonExistentCatalogReq.addIdItem("non_existent_catalog");
    exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.describeNamespace(nonExistentCatalogReq));
    Assertions.assertEquals(404, exception.getCode());

    // test describe a non-existent schema namespace
    DescribeNamespaceRequest nonExistentSchemaReq = new DescribeNamespaceRequest();
    nonExistentSchemaReq.addIdItem(catalog.name());
    nonExistentSchemaReq.addIdItem("non_existent_schema");
    exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.describeNamespace(nonExistentSchemaReq));
    Assertions.assertEquals(404, exception.getCode());
  }

  @Test
  public void testCreateNamespace() {
    String catalogName = GravitinoITUtils.genRandomName("lance_catalog");
    Map<String, String> catalogProps =
        new HashMap<>() {
          {
            put("catalog_key1", "catalog_value1");
          }
        };

    // test create catalog via lance rest namespace client
    CreateNamespaceRequest createNamespaceReq = new CreateNamespaceRequest();
    createNamespaceReq.addIdItem(catalogName);
    createNamespaceReq.setProperties(catalogProps);
    CreateNamespaceResponse createNamespaceResp = ns.createNamespace(createNamespaceReq);

    Catalog catalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(catalog.properties(), createNamespaceResp.getProperties());

    // create catalog again with default mode (create) should fail
    LanceNamespaceException exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.createNamespace(createNamespaceReq));
    Assertions.assertEquals(409, exception.getCode());

    // create catalog again with exist_ok mode should succeed
    createNamespaceReq.setMode(CreateNamespaceRequest.ModeEnum.EXIST_OK);
    createNamespaceResp = ns.createNamespace(createNamespaceReq);
    Assertions.assertEquals(catalog.properties(), createNamespaceResp.getProperties());

    // create catalog again with overwrite mode should succeed and update properties
    Map<String, String> newProps =
        new HashMap<>(catalogProps) {
          {
            put("catalog_key2", "catalog_value2");
          }
        };
    createNamespaceReq.setMode(CreateNamespaceRequest.ModeEnum.OVERWRITE);
    createNamespaceReq.setProperties(newProps);
    createNamespaceResp = ns.createNamespace(createNamespaceReq);

    catalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(catalog.properties(), createNamespaceResp.getProperties());
    Assertions.assertEquals(catalog.properties(), createNamespaceResp.getProperties());

    // test create schema via lance rest namespace client
    CreateNamespaceRequest createSchemaReq = new CreateNamespaceRequest();
    String schemaName = "lance_schema";
    Map<String, String> schemaProps =
        new HashMap<>() {
          {
            put("schema_key1", "schema_value1");
          }
        };
    createSchemaReq.addIdItem(catalogName);
    createSchemaReq.addIdItem(schemaName);
    createSchemaReq.setProperties(schemaProps);
    createNamespaceResp = ns.createNamespace(createSchemaReq);

    Schema schema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schema.properties(), createNamespaceResp.getProperties());

    // create schema again with default mode (create) should fail
    exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.createNamespace(createSchemaReq));
    Assertions.assertEquals(409, exception.getCode());

    // create schema again with exist_ok mode should succeed
    createSchemaReq.setMode(CreateNamespaceRequest.ModeEnum.EXIST_OK);
    createNamespaceResp = ns.createNamespace(createSchemaReq);
    Assertions.assertEquals(schema.properties(), createNamespaceResp.getProperties());

    // create schema again with overwrite mode should succeed and update properties
    Map<String, String> newSchemaProps =
        new HashMap<>(schemaProps) {
          {
            put("schema_key2", "schema_value2");
          }
        };
    createSchemaReq.setMode(CreateNamespaceRequest.ModeEnum.OVERWRITE);
    createSchemaReq.setProperties(newSchemaProps);
    createNamespaceResp = ns.createNamespace(createSchemaReq);

    schema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schema.properties(), createNamespaceResp.getProperties());
  }

  @Test
  public void testDropNamespace() {
    Catalog catalog = createCatalog(GravitinoITUtils.genRandomName("lance_catalog"));
    Schema schema = catalog.asSchemas().createSchema("lance_schema", null, null);

    // test drop a non-existent namespace (catalog) with default mode (FAIL) should fail
    DropNamespaceRequest dropNamespaceReq = new DropNamespaceRequest();
    dropNamespaceReq.addIdItem("non_existent_catalog");
    LanceNamespaceException exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.dropNamespace(dropNamespaceReq));
    Assertions.assertEquals(404, exception.getCode());

    // test drop a non-existent namespace (catalog) with SKIP mode should succeed
    dropNamespaceReq.setMode(DropNamespaceRequest.ModeEnum.SKIP);
    DropNamespaceResponse dropNamespaceResp = ns.dropNamespace(dropNamespaceReq);
    Assertions.assertTrue(dropNamespaceResp.getTransactionId().isEmpty());

    // test drop a non-existent namespace (schema) with default mode (FAIL) should fail
    DropNamespaceRequest dropSchemaReq = new DropNamespaceRequest();
    dropSchemaReq.addIdItem(catalog.name());
    dropSchemaReq.addIdItem("non_existent_schema");
    exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.dropNamespace(dropSchemaReq));
    Assertions.assertEquals(404, exception.getCode());

    // test drop a non-existent namespace (schema) with SKIP mode should succeed
    dropSchemaReq.setMode(DropNamespaceRequest.ModeEnum.SKIP);
    dropNamespaceResp = ns.dropNamespace(dropSchemaReq);
    Assertions.assertTrue(dropNamespaceResp.getTransactionId().isEmpty());

    // test drop a non-empty namespace (catalog) with default behavior (RESTRICT) should fail
    DropNamespaceRequest dropNonEmptyCatalogReq = new DropNamespaceRequest();
    dropNonEmptyCatalogReq.addIdItem(catalog.name());
    exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.dropNamespace(dropNonEmptyCatalogReq));
    Assertions.assertEquals(400, exception.getCode());

    // test drop a non-empty namespace (catalog) with CASCADE behavior should succeed
    dropNonEmptyCatalogReq.setBehavior(DropNamespaceRequest.BehaviorEnum.CASCADE);
    dropNamespaceResp = ns.dropNamespace(dropNonEmptyCatalogReq);
    Assertions.assertTrue(dropNamespaceResp.getTransactionId().isEmpty());
    Assertions.assertFalse(metalake.catalogExists(catalog.name()));

    // recreate catalog, schema, and table for next test
    catalog = createCatalog(catalog.name());
    schema = catalog.asSchemas().createSchema(schema.name(), null, null);
    String tableName = GravitinoITUtils.genRandomName("test_lance_table");
    String tableLocation =
        Path.of(tempDir.toString(), catalog.name(), schema.name(), tableName).toString();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schema.name(), tableName),
            null,
            null,
            ImmutableMap.of("location", tableLocation, "format", "lance"));
    // test drop a non-empty namespace (schema) with default behavior (RESTRICT) should fail
    DropNamespaceRequest dropNonEmptySchemaReq = new DropNamespaceRequest();
    dropNonEmptySchemaReq.addIdItem(catalog.name());
    dropNonEmptySchemaReq.addIdItem(schema.name());
    exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.dropNamespace(dropNonEmptySchemaReq));
    Assertions.assertEquals(400, exception.getCode());
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schema.name()));

    // test drop a non-empty namespace (schema) with CASCADE behavior should succeed
    dropNonEmptySchemaReq.setBehavior(DropNamespaceRequest.BehaviorEnum.CASCADE);
    dropNamespaceResp = ns.dropNamespace(dropNonEmptySchemaReq);
    Assertions.assertTrue(dropNamespaceResp.getTransactionId().isEmpty());
    Assertions.assertFalse(catalog.asSchemas().schemaExists(schema.name()));
  }

  @Test
  public void testNamespaceExists() {
    Catalog catalog = createCatalog(GravitinoITUtils.genRandomName("lance_catalog"));
    Schema schema = catalog.asSchemas().createSchema("lance_schema", null, null);

    // test existing catalog
    NamespaceExistsRequest catalogExistsReq = new NamespaceExistsRequest();
    catalogExistsReq.addIdItem(catalog.name());
    Assertions.assertDoesNotThrow(() -> ns.namespaceExists(catalogExistsReq));

    // test non-existing catalog
    NamespaceExistsRequest nonExistentCatalogReq = new NamespaceExistsRequest();
    nonExistentCatalogReq.addIdItem("non_existent_catalog");
    LanceNamespaceException exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.namespaceExists(nonExistentCatalogReq));
    Assertions.assertEquals(404, exception.getCode());

    // test existing schema
    NamespaceExistsRequest schemaExistsReq = new NamespaceExistsRequest();
    schemaExistsReq.addIdItem(catalog.name());
    schemaExistsReq.addIdItem(schema.name());
    Assertions.assertDoesNotThrow(() -> ns.namespaceExists(schemaExistsReq));

    // test non-existing schema
    NamespaceExistsRequest nonExistentSchemaReq = new NamespaceExistsRequest();
    nonExistentSchemaReq.addIdItem(catalog.name());
    nonExistentSchemaReq.addIdItem("non_existent_schema");
    exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.namespaceExists(nonExistentSchemaReq));
    Assertions.assertEquals(404, exception.getCode());
  }

  @Test
  void testCreateEmptyTable() throws ApiException {
    catalog = createCatalog(CATALOG_NAME);
    createSchema();

    CreateEmptyTableRequest request = new CreateEmptyTableRequest();
    String location = tempDir + "/" + "empty_table/";
    request.setLocation(location);
    request.setProperties(
        ImmutableMap.of(
            "key1", "v1",
            "lance.storage.a", "value_a",
            "lance.storage.b", "value_b"));
    request.setId(List.of(CATALOG_NAME, SCHEMA_NAME, "empty_table"));

    CreateEmptyTableResponse response = ns.createEmptyTable(request);
    Assertions.assertNotNull(response);
    Assertions.assertEquals(location, response.getLocation());
    Assertions.assertEquals("v1", response.getProperties().get("key1"));
    Assertions.assertEquals("value_a", response.getStorageOptions().get("a"));
    Assertions.assertEquals("value_b", response.getStorageOptions().get("b"));

    DescribeTableRequest describeTableRequest = new DescribeTableRequest();
    describeTableRequest.setId(List.of(CATALOG_NAME, SCHEMA_NAME, "empty_table"));

    DescribeTableResponse loadTable = ns.describeTable(describeTableRequest);
    Assertions.assertNotNull(loadTable);
    Assertions.assertEquals(location, loadTable.getLocation());

    // Try to create the same table again should fail
    LanceNamespaceException exception =
        Assertions.assertThrows(
            LanceNamespaceException.class,
            () -> {
              ns.createEmptyTable(request);
            });
    Assertions.assertTrue(exception.getMessage().contains("Table already exists"));
    Assertions.assertEquals(409, exception.getCode());
  }

  @Test
  void testCreateTable() throws IOException, ApiException {
    catalog = createCatalog(CATALOG_NAME);
    createSchema();

    String location = tempDir + "/" + "table/";
    List<String> ids = List.of(CATALOG_NAME, SCHEMA_NAME, "table");
    org.apache.arrow.vector.types.pojo.Schema schema =
        new org.apache.arrow.vector.types.pojo.Schema(
            Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("value", new ArrowType.Utf8())));
    byte[] body = ArrowUtils.generateIpcStream(schema);

    CreateTableRequest request = new CreateTableRequest();
    request.setId(ids);
    request.setLocation(location);
    request.setProperties(
        ImmutableMap.of(
            "key1", "v1",
            "lance.storage.a", "value_a",
            "lance.storage.b", "value_b"));

    CreateTableResponse response = ns.createTable(request, body);
    Assertions.assertNotNull(response);
    Assertions.assertEquals(location, response.getLocation());
    Assertions.assertEquals("v1", response.getProperties().get("key1"));
    Assertions.assertEquals("value_a", response.getStorageOptions().get("a"));
    Assertions.assertEquals("value_b", response.getStorageOptions().get("b"));

    DescribeTableRequest describeTableRequest = new DescribeTableRequest();
    describeTableRequest.setId(ids);
    DescribeTableResponse loadTable = ns.describeTable(describeTableRequest);
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
    Assertions.assertEquals("v1", loadTable.getProperties().get("key1"));
    Assertions.assertEquals("value_a", loadTable.getStorageOptions().get("a"));
    Assertions.assertEquals("value_b", loadTable.getStorageOptions().get("b"));

    // Check overwrite mode
    String newLocation = tempDir + "/" + "table_new/";
    request.setLocation(newLocation);
    request.setMode(CreateTableRequest.ModeEnum.OVERWRITE);
    request.setProperties(
        ImmutableMap.of(
            "key1", "v2",
            "lance.storage.a", "value_va",
            "lance.storage.b", "value_vb"));

    response = Assertions.assertDoesNotThrow(() -> ns.createTable(request, body));

    Assertions.assertNotNull(response);
    Assertions.assertEquals(newLocation, response.getLocation());
    Assertions.assertTrue(response.getProperties().get("key1").equals("v2"));
    Assertions.assertEquals("value_va", response.getStorageOptions().get("a"));
    Assertions.assertEquals("value_vb", response.getStorageOptions().get("b"));
    Assertions.assertTrue(new File(newLocation).exists());
    Assertions.assertFalse(new File(location).exists());

    // Check exist_ok mode
    request.setMode(CreateTableRequest.ModeEnum.EXIST_OK);
    response = Assertions.assertDoesNotThrow(() -> ns.createTable(request, body));

    Assertions.assertNotNull(response);
    Assertions.assertEquals("v2", response.getProperties().get("key1"));
    Assertions.assertEquals("value_va", response.getStorageOptions().get("a"));
    Assertions.assertEquals("value_vb", response.getStorageOptions().get("b"));
    Assertions.assertEquals(newLocation, response.getLocation());
    Assertions.assertTrue(new File(newLocation).exists());

    // Create table again without overwrite or exist_ok should fail
    request.setMode(CreateTableRequest.ModeEnum.CREATE);
    LanceNamespaceException exception =
        Assertions.assertThrows(LanceNamespaceException.class, () -> ns.createTable(request, body));
    Assertions.assertTrue(exception.getMessage().contains("already exists"));
    Assertions.assertEquals(409, exception.getCode());

    // Create a table without location should fail
    CreateTableRequest noLocationRequest = new CreateTableRequest();
    noLocationRequest.setId(List.of(CATALOG_NAME, SCHEMA_NAME, "no_location_table"));
    LanceNamespaceException noLocationException =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.createTable(noLocationRequest, body));
    Assertions.assertTrue(
        noLocationException.getMessage().contains("No location specified for table"));

    // Create table with invalid schema should fail
    byte[] invalidBody = "".getBytes(Charset.defaultCharset());
    CreateTableRequest invalidRequest = new CreateTableRequest();
    invalidRequest.setId(List.of(CATALOG_NAME, SCHEMA_NAME, "invalid_table"));
    invalidRequest.setLocation(tempDir + "/" + "invalid_table/");
    LanceNamespaceException apiException =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.createTable(invalidRequest, invalidBody));
    Assertions.assertTrue(apiException.getMessage().contains("Failed to parse Arrow IPC stream"));
    Assertions.assertEquals(400, apiException.getCode());

    // Create table with wrong ids should fail
    CreateTableRequest wrongIdRequest = new CreateTableRequest();
    wrongIdRequest.setId(List.of(CATALOG_NAME, "wrong_schema")); // This is a schema NOT a table.
    wrongIdRequest.setLocation(tempDir + "/" + "wrong_id_table/");
    LanceNamespaceException wrongIdException =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.createTable(wrongIdRequest, body));
    Assertions.assertTrue(wrongIdException.getMessage().contains("Expected at 3-level namespace"));
    Assertions.assertEquals(400, wrongIdException.getCode());

    // Now test list tables
    ListTablesRequest listRequest = new ListTablesRequest();
    listRequest.setId(List.of(CATALOG_NAME, SCHEMA_NAME));
    var listResponse = ns.listTables(listRequest);
    Set<String> stringSet = listResponse.getTables();
    Assertions.assertEquals(1, stringSet.size());
    Assertions.assertTrue(stringSet.contains(Joiner.on(".").join(ids)));
  }

  @Test
  void testRegisterTable() {
    catalog = createCatalog(CATALOG_NAME);
    createSchema();

    String location = tempDir + "/" + "register/";
    List<String> ids = List.of(CATALOG_NAME, SCHEMA_NAME, "table_register");
    RegisterTableRequest registerTableRequest = new RegisterTableRequest();
    registerTableRequest.setLocation(location);
    registerTableRequest.setMode(ModeEnum.CREATE);
    registerTableRequest.setId(ids);
    registerTableRequest.setProperties(ImmutableMap.of("key1", "value1"));

    RegisterTableResponse response = ns.registerTable(registerTableRequest);
    Assertions.assertNotNull(response);

    DescribeTableRequest describeTableRequest = new DescribeTableRequest();
    describeTableRequest.setId(ids);
    DescribeTableResponse loadTable = ns.describeTable(describeTableRequest);
    Assertions.assertNotNull(loadTable);
    Assertions.assertEquals(location, loadTable.getLocation());
    Assertions.assertTrue(loadTable.getProperties().containsKey("key1"));

    // Test register again with OVERWRITE mode
    String newLocation = tempDir + "/" + "register_new/";
    registerTableRequest.setMode(ModeEnum.OVERWRITE);
    registerTableRequest.setLocation(newLocation);
    response = Assertions.assertDoesNotThrow(() -> ns.registerTable(registerTableRequest));
    Assertions.assertNotNull(response);
    Assertions.assertEquals(newLocation, response.getLocation());

    // Test deregister table
    DeregisterTableRequest deregisterTableRequest = new DeregisterTableRequest();
    deregisterTableRequest.setId(ids);
    DeregisterTableResponse deregisterTableResponse = ns.deregisterTable(deregisterTableRequest);
    Assertions.assertNotNull(deregisterTableResponse);
    Assertions.assertEquals(newLocation, deregisterTableResponse.getLocation());
  }

  @Test
  void testDeregisterNonExistingTable() {
    catalog = createCatalog(CATALOG_NAME);
    createSchema();

    List<String> ids = List.of(CATALOG_NAME, SCHEMA_NAME, "non_existing_table");
    DeregisterTableRequest deregisterTableRequest = new DeregisterTableRequest();
    deregisterTableRequest.setId(ids);

    LanceNamespaceException exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.deregisterTable(deregisterTableRequest));
    Assertions.assertEquals(404, exception.getCode());
    Assertions.assertTrue(exception.getMessage().contains("does not exist"));
    Optional<ErrorResponse> responseOptional = exception.getErrorResponse();
    Assertions.assertTrue(responseOptional.isPresent());
    Assertions.assertEquals(
        NoSuchTableException.class.getSimpleName(), responseOptional.get().getType());

    // Try to create a table and then deregister table
    CreateEmptyTableRequest createEmptyTableRequest = new CreateEmptyTableRequest();
    String location = tempDir + "/" + "to_be_deregistered_table/";
    ids = List.of(CATALOG_NAME, SCHEMA_NAME, "to_be_deregistered_table");
    createEmptyTableRequest.setLocation(location);
    createEmptyTableRequest.setProperties(ImmutableMap.of());
    createEmptyTableRequest.setId(ids);
    CreateEmptyTableResponse response =
        Assertions.assertDoesNotThrow(() -> ns.createEmptyTable(createEmptyTableRequest));
    Assertions.assertNotNull(response);
    Assertions.assertEquals(location, response.getLocation());

    // Now try to deregister
    deregisterTableRequest.setId(ids);
    DeregisterTableResponse deregisterTableResponse =
        Assertions.assertDoesNotThrow(() -> ns.deregisterTable(deregisterTableRequest));
    Assertions.assertNotNull(deregisterTableResponse);
    Assertions.assertEquals(location, deregisterTableResponse.getLocation());
    Assertions.assertTrue(Objects.equals(ids, deregisterTableResponse.getId()));
    Assertions.assertTrue(
        new File(location).exists(), "Data should still exist after deregistering the table.");

    // Now try to describe the table, should fail
    DescribeTableRequest describeTableRequest = new DescribeTableRequest();
    describeTableRequest.setId(ids);
    LanceNamespaceException lanceNamespaceException =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.describeTable(describeTableRequest));
    Assertions.assertEquals(404, lanceNamespaceException.getCode());

    describeTableRequest.setVersion(1L);
    lanceNamespaceException =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.describeTable(describeTableRequest));
    Assertions.assertEquals(406, lanceNamespaceException.getCode());
  }

  @Test
  void testCreateTableIndex() throws IOException {
    catalog = createCatalog(CATALOG_NAME);
    createSchema();
    List<String> ids = List.of(CATALOG_NAME, SCHEMA_NAME, "non_existing_table");

    // We need to create a table first;
    org.apache.arrow.vector.types.pojo.Schema schema =
        new org.apache.arrow.vector.types.pojo.Schema(
            Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("value", new ArrowType.Utf8()),
                new Field(
                    "vector",
                    FieldType.nullable(new ArrowType.FixedSizeList(4)),
                    ImmutableList.of(
                        Field.nullable(
                            "fake", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))))));
    byte[] body = ArrowUtils.generateIpcStream(schema);

    CreateTableRequest request = new CreateTableRequest();
    request.setId(ids);
    request.setLocation(tempDir + "/" + "table_for_index/");
    request.setProperties(
        ImmutableMap.of(
            "key1", "v1",
            "lance.storage.a", "value_a",
            "lance.storage.c", "value_c"));

    CreateTableResponse response = ns.createTable(request, body);
    Assertions.assertEquals(request.getLocation(), response.getLocation());

    writeDataToLance(request.getLocation());

    // Now try to create Btree index on an existing table
    CreateTableIndexRequest createTableIndexRequest = new CreateTableIndexRequest();
    createTableIndexRequest.setId(ids);
    createTableIndexRequest.setIndexType(IndexTypeEnum.BTREE);
    createTableIndexRequest.setColumn("id");
    createTableIndexRequest.setMetricType(MetricTypeEnum.L2);
    CreateTableIndexResponse createTableIndexResponse =
        Assertions.assertDoesNotThrow(() -> ns.createTableIndex(createTableIndexRequest));
    Assertions.assertNotNull(createTableIndexResponse);

    // Now try to create bitmap index on an existing table
    createTableIndexRequest.setIndexType(IndexTypeEnum.BITMAP);
    createTableIndexRequest.setColumn("value");
    createTableIndexResponse =
        Assertions.assertDoesNotThrow(() -> ns.createTableIndex(createTableIndexRequest));
    Assertions.assertNotNull(createTableIndexResponse);
    List<String> indices = listIndices(request.getLocation());
    Assertions.assertEquals(2, indices.size());
    // Now try to create vector index on an existing table
    createTableIndexRequest.setIndexType(IndexTypeEnum.IVF_FLAT);
    createTableIndexRequest.setColumn("vector");
    createTableIndexResponse =
        Assertions.assertDoesNotThrow(() -> ns.createTableIndex(createTableIndexRequest));
    Assertions.assertNotNull(createTableIndexResponse);

    ListTableIndicesRequest listTableIndicesRequest = new ListTableIndicesRequest();
    listTableIndicesRequest.setId(ids);
    ListTableIndicesResponse listTableIndicesResponse =
        ns.listTableIndices(listTableIndicesRequest);
    Assertions.assertEquals(3, listTableIndicesResponse.getIndexes().size());
    List<String> expectedIndexName = listIndices(request.getLocation());
    for (IndexContent indexContent : listTableIndicesResponse.getIndexes()) {
      Assertions.assertTrue(
          expectedIndexName.contains(indexContent.getIndexName()),
          "Index name should be in the expected index names.");
      if (indexContent.getIndexName().equals("id_idx")) {
        Assertions.assertEquals("id", indexContent.getColumns().get(0));
      } else if (indexContent.getIndexName().equals("value_idx")) {
        Assertions.assertEquals("value", indexContent.getColumns().get(0));
      } else if (indexContent.getIndexName().equals("vector_idx")) {
        Assertions.assertEquals("vector", indexContent.getColumns().get(0));
      }
    }

    // create another table to test other index types
    ids = List.of(CATALOG_NAME, SCHEMA_NAME, "table_for_other_indexes");
    request.setId(ids);
    request.setLocation(tempDir + "/" + "table_for_other_indexes/");
    response = ns.createTable(request, body);
    Assertions.assertEquals(request.getLocation(), response.getLocation());
    writeDataToLance(request.getLocation());

    // Now try to create FTS index on an existing table
    createTableIndexRequest.setId(ids);
    createTableIndexRequest.setIndexType(IndexTypeEnum.FTS);
    createTableIndexRequest.setColumn("value");

    LanceNamespaceException exception =
        Assertions.assertThrows(
            LanceNamespaceException.class, () -> ns.createTableIndex(createTableIndexRequest));
    // com.lancedb.lance.index.IndexType does not have FTS yet, so it should throw exception
    Assertions.assertTrue(
        exception.getMessage().contains("No enum constant com.lancedb.lance.index.IndexType.FTS"));
  }

  private List<String> listIndices(String lanceTableLocation) {
    try (Dataset dataset = Dataset.open(lanceTableLocation)) {
      return dataset.listIndexes();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void writeDataToLance(String tableLocation) {
    try (Dataset dataset = Dataset.open(tableLocation)) {
      org.apache.arrow.vector.types.pojo.Schema lanceSchema = dataset.getSchema();
      Transaction trans =
          dataset
              .newTransactionBuilder()
              .operation(
                  Append.builder()
                      .fragments(
                          createFragmentMetadata(tableLocation, generateLanceData(), lanceSchema))
                      .build())
              .writeParams(ImmutableMap.of())
              .build();

      Dataset newDataset = dataset.commitTransaction(trans);

      try (LanceScanner scanner =
          newDataset.newScan(
              new ScanOptions.Builder()
                  .columns(Arrays.asList("id", "value", "vector"))
                  .batchSize(1000)
                  .build())) {

        List<LanceDataValue> dataValues = com.google.common.collect.Lists.newArrayList();
        try (ArrowReader reader = scanner.scanBatches()) {
          while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<FieldVector> fieldVectors = root.getFieldVectors();

            IntVector ids = (IntVector) fieldVectors.get(0);
            VarCharVector values = (VarCharVector) fieldVectors.get(1);
            FixedSizeListVector vectors = (FixedSizeListVector) fieldVectors.get(2);

            for (int i = 0; i < root.getRowCount(); i++) {
              int id = ids.get(i);
              String value = new String(values.get(i), StandardCharsets.UTF_8);
              List<Float> vector = com.google.common.collect.Lists.newArrayList();
              for (int j = 0; j < 4; j++) {
                Float floatValue = ((Float4Vector) vectors.getDataVector()).get(i * 4 + j);
                vector.add(floatValue);
              }

              dataValues.add(new LanceDataValue(id, value, vector));
            }
          }
        }

        Assertions.assertEquals(5120, dataValues.size());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<LanceDataValue> generateLanceData() {
    List<LanceDataValue> updates = Lists.newArrayList();
    Random random = new Random();
    for (int i = 0; i < 5120; i++) {
      LanceDataValue data =
          new LanceDataValue(
              i,
              "value_" + i,
              Arrays.asList(
                  (float) random.nextInt(10000),
                  (float) random.nextInt(10000),
                  (float) random.nextInt(10000),
                  (float) random.nextInt(10000)));
      updates.add(data);
    }

    return updates;
  }

  private List<FragmentMetadata> createFragmentMetadata(
      String tableLocation,
      List<LanceDataValue> updates,
      org.apache.arrow.vector.types.pojo.Schema schema)
      throws JsonProcessingException {
    List<FragmentMetadata> fragmentMetas;
    int count = 0;
    RootAllocator rootAllocator = new RootAllocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, rootAllocator)) {
      for (FieldVector vector : root.getFieldVectors()) {
        vector.setInitialCapacity(count);
      }
      root.allocateNew();

      IntVector ids = (IntVector) root.getVector("id");
      VarCharVector values = (VarCharVector) root.getVector("value");
      FixedSizeListVector vectors = (FixedSizeListVector) root.getVector("vector");
      vectors.allocateNew();
      Float4Vector dataVector = (Float4Vector) vectors.getDataVector();

      int index = 0;
      for (LanceDataValue data : updates) {
        ids.setSafe(index, data.id);
        values.setSafe(index, data.value.getBytes(StandardCharsets.UTF_8));
        vectors.setNotNull(index);
        for (int i = 0; i < 4; i++) {
          Float floatValue = data.vector.get(i);
          dataVector.setSafe(index * 4 + i, floatValue);
        }
        index++;
      }
      root.setRowCount(index);

      fragmentMetas =
          Fragment.create(tableLocation, rootAllocator, root, new WriteParams.Builder().build());
      return fragmentMetas;
    }
  }

  static class LanceDataValue {
    public Integer id;
    public String value;
    public List<Float> vector;

    public LanceDataValue(Integer id, String value, List<Float> vector) {
      this.id = id;
      this.value = value;
      this.vector = vector;
    }
  }

  private GravitinoMetalake createMetalake(String metalakeName) {
    return client.createMetalake(metalakeName, "metalake for lance rest service tests", null);
  }

  private Catalog createCatalog(String catalogName) {
    return metalake.createCatalog(
        catalogName,
        Catalog.Type.RELATIONAL,
        "generic-lakehouse",
        "catalog for lance rest service tests",
        properties);
  }

  private void createSchema() {
    Map<String, String> schemaProperties = Maps.newHashMap();
    String comment = "comment";
    catalog.asSchemas().createSchema(SCHEMA_NAME, comment, schemaProperties);
    catalog.asSchemas().loadSchema(SCHEMA_NAME);
  }

  private String getLanceRestServiceUrl() {
    return String.format("http://%s:%d/lance", "localhost", getLanceRESTServerPort());
  }
}
