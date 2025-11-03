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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.LanceNamespaces;
import com.lancedb.lance.namespace.model.DescribeNamespaceRequest;
import com.lancedb.lance.namespace.model.DescribeNamespaceResponse;
import com.lancedb.lance.namespace.model.ListNamespacesRequest;
import com.lancedb.lance.namespace.model.ListNamespacesResponse;
import com.lancedb.lance.namespace.rest.RestNamespaceConfig;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
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
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private LanceNamespace ns;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.ignoreLanceAuxRestService = false;
    super.startIntegrationTest();
    this.metalake = createMetalake(getLanceRESTServerMetalakeName());

    HashMap<String, String> props = Maps.newHashMap();
    props.put(RestNamespaceConfig.URI, getLanceRestServiceUrl());
    props.put(RestNamespaceConfig.DELIMITER, RestNamespaceConfig.DELIMITER_DEFAULT);
    this.ns = LanceNamespaces.connect("rest", props, null, allocator);
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
