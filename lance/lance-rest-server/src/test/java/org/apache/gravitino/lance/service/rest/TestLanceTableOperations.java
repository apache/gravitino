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
package org.apache.gravitino.lance.service.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.model.CreateEmptyTableRequest;
import com.lancedb.lance.namespace.model.CreateEmptyTableResponse;
import com.lancedb.lance.namespace.model.CreateTableIndexRequest;
import com.lancedb.lance.namespace.model.CreateTableIndexResponse;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DeregisterTableRequest;
import com.lancedb.lance.namespace.model.DeregisterTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.ErrorResponse;
import com.lancedb.lance.namespace.model.IndexContent;
import com.lancedb.lance.namespace.model.ListTableIndicesRequest;
import com.lancedb.lance.namespace.model.ListTableIndicesResponse;
import com.lancedb.lance.namespace.model.RegisterTableRequest;
import com.lancedb.lance.namespace.model.RegisterTableResponse;
import java.io.IOException;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.lance.common.ops.LanceTableOperations;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestLanceTableOperations extends JerseyTest {

  private static NamespaceWrapper namespaceWrapper = mock(NamespaceWrapper.class);
  private static org.apache.gravitino.lance.common.ops.LanceTableOperations tableOps =
      mock(LanceTableOperations.class);

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(org.apache.gravitino.lance.service.rest.LanceTableOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(namespaceWrapper).to(NamespaceWrapper.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @BeforeAll
  public static void setup() {
    when(namespaceWrapper.asTableOps()).thenReturn(tableOps);
  }

  @Test
  void testCreateTable() {
    String tableIds = "catalog.scheme.create_table";
    String delimiter = ".";

    // Test normal
    CreateTableResponse createTableResponse = new CreateTableResponse();
    when(tableOps.createTable(any(), any(), any(), any(), any(), any()))
        .thenReturn(createTableResponse);

    byte[] bytes = new byte[] {0x01, 0x02, 0x03};
    Response resp =
        target(String.format("/v1/table/%s/create", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(bytes, "application/vnd.apache.arrow.stream"));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    // Test illegal argument
    when(tableOps.createTable(any(), any(), any(), any(), any(), any()))
        .thenThrow(new IllegalArgumentException("Illegal argument"));

    resp =
        target(String.format("/v1/table/%s/create", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(bytes, "application/vnd.apache.arrow.stream"));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    // Test runtime exception
    Mockito.reset(tableOps);
    when(tableOps.createTable(any(), any(), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Runtime exception"));
    resp =
        target(String.format("/v1/table/%s/create", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(bytes, "application/vnd.apache.arrow.stream"));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Runtime exception", errorResp.getError());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
  }

  @Test
  void testCreateEmptyTable() {
    String tableIds = "catalog.scheme.create_empty_table";
    String delimiter = ".";

    // Test normal
    CreateEmptyTableResponse createTableResponse = new CreateEmptyTableResponse();
    createTableResponse.setLocation("/path/to/table");
    createTableResponse.setProperties(ImmutableMap.of("key", "value"));
    when(tableOps.createEmptyTable(any(), any(), any(), any())).thenReturn(createTableResponse);

    CreateEmptyTableRequest tableRequest = new CreateEmptyTableRequest();
    tableRequest.setLocation("/path/to/table");

    Response resp =
        target(String.format("/v1/table/%s/create-empty", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    CreateEmptyTableResponse response = resp.readEntity(CreateEmptyTableResponse.class);
    Assertions.assertEquals(createTableResponse.getLocation(), response.getLocation());
    Assertions.assertEquals(createTableResponse.getProperties(), response.getProperties());

    Mockito.reset(tableOps);
    // Test illegal argument
    when(tableOps.createEmptyTable(any(), any(), any(), any()))
        .thenThrow(new IllegalArgumentException("Illegal argument"));

    resp =
        target(String.format("/v1/table/%s/create-empty", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    // Test runtime exception
    Mockito.reset(tableOps);
    when(tableOps.createEmptyTable(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Runtime exception"));
    resp =
        target(String.format("/v1/table/%s/create-empty", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Runtime exception", errorResp.getError());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
  }

  @Test
  void testRegisterTable() {
    String tableIds = "catalog.scheme.register_table";
    String delimiter = ".";

    // Test normal
    RegisterTableResponse registerTableResponse = new RegisterTableResponse();
    registerTableResponse.setLocation("/path/to/registered_table");
    registerTableResponse.setProperties(ImmutableMap.of("key", "value"));
    when(tableOps.registerTable(any(), any(), any(), any())).thenReturn(registerTableResponse);

    RegisterTableRequest tableRequest = new RegisterTableRequest();
    tableRequest.setLocation("/path/to/registered_table");
    tableRequest.setMode(RegisterTableRequest.ModeEnum.CREATE);

    Response resp =
        target(String.format("/v1/table/%s/register", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    RegisterTableResponse response = resp.readEntity(RegisterTableResponse.class);
    Assertions.assertEquals(registerTableResponse.getLocation(), response.getLocation());
    Assertions.assertEquals(registerTableResponse.getProperties(), response.getProperties());

    // Test illegal argument
    Mockito.reset(tableOps);
    when(tableOps.registerTable(any(), any(), any(), any()))
        .thenThrow(new IllegalArgumentException("Illegal argument"));
    resp =
        target(String.format("/v1/table/%s/register", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    // Test runtime exception
    Mockito.reset(tableOps);
    when(tableOps.registerTable(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Runtime exception"));
    resp =
        target(String.format("/v1/table/%s/register", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Runtime exception", errorResp.getError());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
  }

  @Test
  void testDeregisterTable() {
    String tableIds = "catalog.scheme.deregister_table";
    String delimiter = ".";

    DeregisterTableRequest tableRequest = new DeregisterTableRequest();

    DeregisterTableResponse deregisterTableResponse = new DeregisterTableResponse();
    deregisterTableResponse.setLocation("/path/to/deregistered_table");
    deregisterTableResponse.setProperties(ImmutableMap.of("key", "value"));
    // Test normal
    when(tableOps.deregisterTable(any(), any())).thenReturn(deregisterTableResponse);

    Response resp =
        target(String.format("/v1/table/%s/deregister", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    DeregisterTableResponse response = resp.readEntity(DeregisterTableResponse.class);
    Assertions.assertEquals(deregisterTableResponse.getLocation(), response.getLocation());
    Assertions.assertEquals(deregisterTableResponse.getProperties(), response.getProperties());

    // Test illegal argument
    Mockito.reset(tableOps);
    when(tableOps.deregisterTable(any(), any()))
        .thenThrow(new IllegalArgumentException("Illegal argument"));
    resp =
        target(String.format("/v1/table/%s/deregister", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    // Test not found exception
    Mockito.reset(tableOps);
    when(tableOps.deregisterTable(any(), any()))
        .thenThrow(
            LanceNamespaceException.notFound(
                "Table not found", "NoSuchTableException", tableIds, ""));
    resp =
        target(String.format("/v1/table/%s/deregister", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Test runtime exception
    Mockito.reset(tableOps);
    when(tableOps.deregisterTable(any(), any()))
        .thenThrow(new RuntimeException("Runtime exception"));
    resp =
        target(String.format("/v1/table/%s/deregister", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Runtime exception", errorResp.getError());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
  }

  @Test
  void testDescribeTable() {
    String tableIds = "catalog.scheme.describe_table";
    String delimiter = ".";

    // Test normal
    DescribeTableResponse createTableResponse = new DescribeTableResponse();
    createTableResponse.setLocation("/path/to/describe_table");
    createTableResponse.setProperties(ImmutableMap.of("key", "value"));
    when(tableOps.describeTable(any(), any(), any())).thenReturn(createTableResponse);

    DescribeTableRequest tableRequest = new DescribeTableRequest();
    Response resp =
        target(String.format("/v1/table/%s/describe", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    DescribeTableResponse response = resp.readEntity(DescribeTableResponse.class);
    Assertions.assertEquals(createTableResponse.getLocation(), response.getLocation());
    Assertions.assertEquals(createTableResponse.getProperties(), response.getProperties());

    // Test not found exception
    Mockito.reset(tableOps);
    when(tableOps.describeTable(any(), any(), any()))
        .thenThrow(
            LanceNamespaceException.notFound(
                "Table not found", "NoSuchTableException", tableIds, ""));
    resp =
        target(String.format("/v1/table/%s/describe", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Test runtime exception
    Mockito.reset(tableOps);
    when(tableOps.describeTable(any(), any(), any()))
        .thenThrow(new RuntimeException("Runtime exception"));
    resp =
        target(String.format("/v1/table/%s/describe", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Runtime exception", errorResp.getError());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
  }

  @Test
  void testCreateTableIndex() {
    String tableIds = "catalog.scheme.to_create_index_table";
    String delimiter = ".";

    // Test normal
    CreateTableIndexRequest tableRequest = new CreateTableIndexRequest();

    CreateTableIndexResponse response = new CreateTableIndexResponse();
    response.setProperties(ImmutableMap.of("key", "value"));
    when(tableOps.createTableIndex(any(), any(), any())).thenReturn(response);

    Response resp =
        target(String.format("/v1/table/%s/create_index", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    response = resp.readEntity(CreateTableIndexResponse.class);
    Assertions.assertEquals(response.getProperties(), response.getProperties());

    Mockito.reset(tableOps);
    // Test illegal argument
    when(tableOps.createTableIndex(any(), any(), any()))
        .thenThrow(new IllegalArgumentException("Illegal argument"));

    resp =
        target(String.format("/v1/table/%s/create_index", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    // Test runtime exception
    Mockito.reset(tableOps);
    when(tableOps.createTableIndex(any(), any(), any()))
        .thenThrow(new RuntimeException("Runtime exception"));
    resp =
        target(String.format("/v1/table/%s/create_index", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Runtime exception", errorResp.getError());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
  }

  @Test
  void testListTableIndices() {
    String tableIds = "catalog.scheme.to_list_index_table";
    String delimiter = ".";

    ListTableIndicesRequest tableRequest = new ListTableIndicesRequest();

    ListTableIndicesResponse response = new ListTableIndicesResponse();
    IndexContent indexContent = new IndexContent();
    indexContent.setIndexName("test_index");
    indexContent.setColumns(List.of("col1"));
    response.setIndexes(List.of(indexContent));
    when(tableOps.listTableIndices(any(), any(), any())).thenReturn(response);

    Response resp =
        target(String.format("/v1/table/%s/index/list", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    ListTableIndicesResponse actualResponse = resp.readEntity(ListTableIndicesResponse.class);
    Assertions.assertEquals(1, actualResponse.getIndexes().size());
    Assertions.assertEquals("test_index", actualResponse.getIndexes().get(0).getIndexName());
    Assertions.assertEquals(List.of("col1"), actualResponse.getIndexes().get(0).getColumns());

    Mockito.reset(tableOps);

    // Test illegal argument
    when(tableOps.listTableIndices(any(), any(), any()))
        .thenThrow(new IllegalArgumentException("Illegal argument"));
    resp =
        target(String.format("/v1/table/%s/index/list", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    // Test
    Mockito.reset(tableOps);
    when(tableOps.listTableIndices(any(), any(), any()))
        .thenThrow(
            LanceNamespaceException.notFound(
                "Table not found", "NoSuchTableException", tableIds, ""));
    resp =
        target(String.format("/v1/table/%s/index/list", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());
  }
}
