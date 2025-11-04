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
import com.google.common.collect.Sets;
import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.LanceNamespaces;
import com.lancedb.lance.namespace.model.CreateNamespaceRequest;
import com.lancedb.lance.namespace.model.CreateNamespaceResponse;
import com.lancedb.lance.namespace.model.DescribeNamespaceRequest;
import com.lancedb.lance.namespace.model.DescribeNamespaceResponse;
import com.lancedb.lance.namespace.model.DropNamespaceRequest;
import com.lancedb.lance.namespace.model.DropNamespaceResponse;
import com.lancedb.lance.namespace.model.ListNamespacesRequest;
import com.lancedb.lance.namespace.model.ListNamespacesResponse;
import com.lancedb.lance.namespace.model.NamespaceExistsRequest;
import com.lancedb.lance.namespace.rest.RestNamespaceConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LanceRESTServiceIT extends BaseIT {

  private GravitinoMetalake metalake;
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
    // deleteOnExit
    tempDir.toFile().deleteOnExit();
  }

  @AfterAll
  public void clean() {
    client.dropMetalake(getLanceRESTServerMetalakeName(), true);
    tempDir.toFile().deleteOnExit();
  }

  @AfterEach
  public void clearMetalake() {
    Arrays.stream(metalake.listCatalogs()).forEach(c -> metalake.dropCatalog(c, true));
    tempDir.toFile().deleteOnExit();
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

  private String getLanceRestServiceUrl() {
    return String.format("http://%s:%d/lance", "localhost", getLanceRESTServerPort());
  }
}
