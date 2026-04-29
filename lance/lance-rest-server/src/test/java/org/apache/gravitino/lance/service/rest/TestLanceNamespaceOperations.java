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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.lance.common.ops.LanceTableOperations;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.lance.common.utils.LanceConstants;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.AlterColumnsEntry;
import org.lance.namespace.model.AlterTableAlterColumnsRequest;
import org.lance.namespace.model.AlterTableAlterColumnsResponse;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.AlterTableDropColumnsResponse;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.CreateEmptyTableResponse;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.CreateTableResponse;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.ErrorResponse;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.RegisterTableRequest;
import org.lance.namespace.model.RegisterTableResponse;
import org.mockito.Mockito;

@SuppressWarnings("deprecation")
public class TestLanceNamespaceOperations extends JerseyTest {
  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private static NamespaceWrapper namespaceWrapper = mock(NamespaceWrapper.class);
  private static org.apache.gravitino.lance.common.ops.LanceNamespaceOperations namespaceOps =
      mock(org.apache.gravitino.lance.common.ops.LanceNamespaceOperations.class);
  private static LanceTableOperations tableOps = mock(LanceTableOperations.class);

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(LanceNamespaceOperations.class);
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
    when(namespaceWrapper.asNamespaceOps()).thenReturn(namespaceOps);
    when(namespaceWrapper.asTableOps()).thenReturn(tableOps);
    org.apache.gravitino.lance.common.config.LanceConfig lanceConfig =
        mock(org.apache.gravitino.lance.common.config.LanceConfig.class);
    when(namespaceWrapper.config()).thenReturn(lanceConfig);
    when(lanceConfig.getGravitinoMetalake()).thenReturn("test-metalake");
  }

  @Test
  public void testListNamespaces() {
    String namespaceId = "ns1.ns2";
    String delimiter = ".";
    ListNamespacesResponse listNamespacesResp = new ListNamespacesResponse();
    listNamespacesResp.setNamespaces(Sets.newHashSet(namespaceId.split(delimiter)));

    when(namespaceOps.listNamespaces(any(), any(), any(), any())).thenReturn(listNamespacesResp);

    Response resp =
        target("/v1/namespace/ns1.ns2/list")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .get();

    Mockito.verify(namespaceOps)
        .listNamespaces(eq(namespaceId), eq(Pattern.quote(delimiter)), any(), any());
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ListNamespacesResponse respEntity = resp.readEntity(ListNamespacesResponse.class);
    Assertions.assertEquals(listNamespacesResp.getNamespaces(), respEntity.getNamespaces());
    Assertions.assertEquals(listNamespacesResp.getPageToken(), respEntity.getPageToken());

    // list namespaces under root
    resp =
        target("/v1/namespace/./list")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .get();

    Mockito.verify(namespaceOps)
        .listNamespaces(eq("."), eq(Pattern.quote(delimiter)), any(), any());
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    respEntity = resp.readEntity(ListNamespacesResponse.class);
    Assertions.assertEquals(listNamespacesResp.getNamespaces(), respEntity.getNamespaces());
    Assertions.assertEquals(listNamespacesResp.getPageToken(), respEntity.getPageToken());

    // list namespaces via root endpoint
    resp =
        target("/v1/namespace/list")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .get();

    Mockito.verify(namespaceOps).listNamespaces(eq(""), eq(Pattern.quote(delimiter)), any(), any());
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    respEntity = resp.readEntity(ListNamespacesResponse.class);
    Assertions.assertEquals(listNamespacesResp.getNamespaces(), respEntity.getNamespaces());
    Assertions.assertEquals(listNamespacesResp.getPageToken(), respEntity.getPageToken());

    // test throw exception
    when(namespaceOps.listNamespaces(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Test exception"));
    resp =
        target("/v1/namespace/ns1.ns2/list")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(18, errorResp.getCode());
    Assertions.assertEquals("Test exception", errorResp.getError());
    Assertions.assertEquals("ns1.ns2", errorResp.getInstance());
    Assertions.assertNotNull(errorResp.getDetail());
    Assertions.assertTrue(errorResp.getDetail().contains("Test exception"));

    // root endpoint should use explicit root identifier instead of delimiter in error instance
    resp =
        target("/v1/namespace/list")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .get();
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("", errorResp.getInstance());
  }

  @Test
  public void testDescribeNamespace() {
    String namespaceId = "ns1.ns2";
    String delimiter = ".";
    DescribeNamespaceResponse describeNamespaceResp = new DescribeNamespaceResponse();
    describeNamespaceResp.setProperties(ImmutableMap.of("key", "value"));

    when(namespaceOps.describeNamespace(any(), any())).thenReturn(describeNamespaceResp);

    Response resp =
        target("/v1/namespace/ns1.ns2/describe")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);

    Mockito.verify(namespaceOps).describeNamespace(eq(namespaceId), eq(Pattern.quote(delimiter)));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DescribeNamespaceResponse respEntity = resp.readEntity(DescribeNamespaceResponse.class);
    Assertions.assertEquals(describeNamespaceResp.getProperties(), respEntity.getProperties());

    // describe namespace via root endpoint
    resp =
        target("/v1/namespace/describe")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);

    Mockito.verify(namespaceOps).describeNamespace(eq(""), eq(Pattern.quote(delimiter)));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    respEntity = resp.readEntity(DescribeNamespaceResponse.class);
    Assertions.assertEquals(describeNamespaceResp.getProperties(), respEntity.getProperties());

    // test throw exception
    when(namespaceOps.describeNamespace(any(), any()))
        .thenThrow(new RuntimeException("Test exception"));
    resp =
        target("/v1/namespace/ns1.ns2/describe")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(18, errorResp.getCode());
    Assertions.assertEquals("Test exception", errorResp.getError());

    // root endpoint should use explicit root identifier instead of delimiter in error instance
    resp =
        target("/v1/namespace/describe")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("", errorResp.getInstance());
  }

  @Test
  public void testCreateNamespace() {
    String namespaceId = "ns1.ns2";
    String delimiter = ".";
    CreateNamespaceRequest createNamespaceReq = new CreateNamespaceRequest();
    createNamespaceReq.setProperties(ImmutableMap.of("key", "value"));

    CreateNamespaceResponse createNamespaceResp = new CreateNamespaceResponse();
    createNamespaceResp.setProperties(ImmutableMap.of("key", "value"));

    when(namespaceOps.createNamespace(any(), any(), any(), any())).thenReturn(createNamespaceResp);

    Response resp =
        target("/v1/namespace/ns1.ns2/create")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(createNamespaceReq, MediaType.APPLICATION_JSON_TYPE));

    Mockito.verify(namespaceOps)
        .createNamespace(
            eq(namespaceId),
            eq(Pattern.quote(delimiter)),
            eq("create"),
            eq(createNamespaceReq.getProperties()));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    CreateNamespaceResponse respEntity = resp.readEntity(CreateNamespaceResponse.class);
    Assertions.assertEquals(createNamespaceResp.getProperties(), respEntity.getProperties());

    // test throw exception
    when(namespaceOps.createNamespace(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Test exception"));
    resp =
        target("/v1/namespace/ns1.ns2/create")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(createNamespaceReq, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(18, errorResp.getCode());
    Assertions.assertEquals("Test exception", errorResp.getError());
  }

  @Test
  public void testNamespaceExists() {
    String namespaceId = "ns1.ns2";
    String delimiter = ".";

    doNothing().when(namespaceOps).namespaceExists(any(), any());

    Response resp =
        target("/v1/namespace/ns1.ns2/exists")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);

    Mockito.verify(namespaceOps).namespaceExists(eq(namespaceId), eq(Pattern.quote(delimiter)));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // test throw exception
    doThrow(new NoSuchCatalogException("Not found"))
        .when(namespaceOps)
        .namespaceExists(any(), any());
    resp =
        target("/v1/namespace/ns1.ns2/exists")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(1, errorResp.getCode());
    Assertions.assertEquals("Not found", errorResp.getError());
  }

  @Test
  public void testDropNamespace() {
    String namespaceId = "ns1.ns2";
    String delimiter = ".";
    DropNamespaceRequest dropNamespaceReq = new DropNamespaceRequest();

    DropNamespaceResponse dropNamespaceResp = new DropNamespaceResponse();
    when(namespaceOps.dropNamespace(any(), any(), any(), any())).thenReturn(dropNamespaceResp);

    Response resp =
        target("/v1/namespace/ns1.ns2/drop")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(dropNamespaceReq, MediaType.APPLICATION_JSON_TYPE));

    Mockito.verify(namespaceOps)
        .dropNamespace(eq(namespaceId), eq(Pattern.quote(delimiter)), eq("fail"), eq("restrict"));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    // test throw exception
    when(namespaceOps.dropNamespace(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Test exception"));
    resp =
        target("/v1/namespace/ns1.ns2/drop")
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(dropNamespaceReq, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(18, errorResp.getCode());
    Assertions.assertEquals("Test exception", errorResp.getError());
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
  }

  @Test
  void testCreateEmptyTable() {
    String tableIds = "catalog.scheme.create_empty_table";
    String delimiter = ".";

    // Test normal
    CreateEmptyTableResponse createTableResponse = new CreateEmptyTableResponse();
    createTableResponse.setLocation("/path/to/table");
    createTableResponse.setStorageOptions(ImmutableMap.of("key", "value"));
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
    Assertions.assertEquals(createTableResponse.getStorageOptions(), response.getStorageOptions());

    Mockito.verify(tableOps)
        .createEmptyTable(eq(tableIds), eq(delimiter), eq("/path/to/table"), eq(Map.of()));

    // Backward compatibility: request-body properties should still be accepted.
    Mockito.reset(tableOps);
    when(tableOps.createEmptyTable(any(), any(), any(), any())).thenReturn(createTableResponse);
    String bodyWithProperties =
        "{"
            + "\"id\":[\"catalog\",\"scheme\",\"create_empty_table\"],"
            + "\"location\":\"/path/to/table\","
            + "\"properties\":{\"k1\":\"v1\",\"k2\":2}"
            + "}";
    resp =
        target(String.format("/v1/table/%s/create-empty", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(bodyWithProperties, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Mockito.verify(tableOps)
        .createEmptyTable(
            eq(tableIds),
            eq(delimiter),
            eq("/path/to/table"),
            argThat(
                (Map<String, String> props) ->
                    "v1".equals(props.get("k1"))
                        && "2".equals(props.get("k2"))
                        && props.size() == 2));

    // Header properties should override body properties on key conflicts.
    Mockito.reset(tableOps);
    when(tableOps.createEmptyTable(any(), any(), any(), any())).thenReturn(createTableResponse);
    String bodyWithOverlappedProperties =
        "{"
            + "\"id\":[\"catalog\",\"scheme\",\"create_empty_table\"],"
            + "\"location\":\"/path/to/table\","
            + "\"properties\":{\"k1\":\"body\",\"k2\":\"body2\"}"
            + "}";
    resp =
        target(String.format("/v1/table/%s/create-empty", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .header(
                LanceConstants.LANCE_TABLE_PROPERTIES_PREFIX_HEADER,
                "{\"k1\":\"header\",\"k3\":\"v3\"}")
            .post(Entity.entity(bodyWithOverlappedProperties, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Mockito.verify(tableOps)
        .createEmptyTable(
            eq(tableIds),
            eq(delimiter),
            eq("/path/to/table"),
            argThat(
                (Map<String, String> props) ->
                    "header".equals(props.get("k1"))
                        && "body2".equals(props.get("k2"))
                        && "v3".equals(props.get("k3"))
                        && props.size() == 3));

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
    tableRequest.setMode("create");

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
  }

  @Test
  void testRegisterTableSetsRegisterPropertyToTrue() {
    String tableIds = "catalog.scheme.register_table_with_property";
    String delimiter = ".";

    // Reset mock to clear any previous test state
    Mockito.reset(tableOps);

    // Test that the "register" property is set to "true"
    RegisterTableResponse registerTableResponse = new RegisterTableResponse();
    registerTableResponse.setLocation("/path/to/registered_table");
    registerTableResponse.setProperties(
        ImmutableMap.of("key", "value", LanceConstants.LANCE_TABLE_REGISTER, "true"));
    when(tableOps.registerTable(any(), any(), any(), any())).thenReturn(registerTableResponse);

    RegisterTableRequest tableRequest = new RegisterTableRequest();
    tableRequest.setLocation("/path/to/registered_table");
    tableRequest.setMode("create");
    tableRequest.setProperties(ImmutableMap.of("custom-key", "custom-value"));

    Response resp =
        target(String.format("/v1/table/%s/register", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Verify that registerTable was called with properties containing "register": "true"
    Mockito.verify(tableOps)
        .registerTable(
            eq(tableIds),
            eq("create"),
            eq(delimiter),
            Mockito.argThat(
                props ->
                    props != null
                        && "true".equals(props.get(LanceConstants.LANCE_TABLE_REGISTER))
                        && "/path/to/registered_table".equals(props.get("location"))
                        && "custom-value".equals(props.get("custom-key"))));
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
        .thenThrow(new TableNotFoundException("Table not found", "", tableIds));
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
  }

  @Test
  void testDescribeTable() {
    String tableIds = "catalog.scheme.describe_table";
    String delimiter = ".";

    // Test normal
    DescribeTableResponse createTableResponse = new DescribeTableResponse();
    createTableResponse.setLocation("/path/to/describe_table");
    createTableResponse.setMetadata(ImmutableMap.of("key", "value"));
    when(tableOps.describeTable(any(), any(), any(), any())).thenReturn(createTableResponse);

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
    Assertions.assertEquals(createTableResponse.getMetadata(), response.getMetadata());

    // Test not found exception
    Mockito.reset(tableOps);
    when(tableOps.describeTable(any(), any(), any(), any()))
        .thenThrow(new TableNotFoundException("Table not found", "", tableIds));
    resp =
        target(String.format("/v1/table/%s/describe", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Test runtime exception
    Mockito.reset(tableOps);
    when(tableOps.describeTable(any(), any(), any(), any()))
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
  }

  @Test
  void testTableExists() {
    String tableIds = "catalog.scheme.table_exists";
    String delimiter = ".";

    doReturn(true).when(tableOps).tableExists(any(), any());

    Response resp =
        target(String.format("/v1/table/%s/exists", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // test throw exception
    doThrow(new TableNotFoundException("Table not found", "", tableIds))
        .when(tableOps)
        .tableExists(any(), any());
    resp =
        target(String.format("/v1/table/%s/exists", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(4, errorResp.getCode());
    Assertions.assertEquals("Table not found", errorResp.getError());

    // Test runtime exception
    Mockito.reset(tableOps);
    doThrow(new RuntimeException("Runtime exception")).when(tableOps).tableExists(any(), any());
    resp =
        target(String.format("/v1/table/%s/exists", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
  }

  @Test
  void testDropTable() {
    String tableIds = "catalog.scheme.drop_table";
    String delimiter = ".";

    DropTableResponse dropTableResponse = new DropTableResponse();
    dropTableResponse.setId(Lists.newArrayList("catalog", "scheme", "drop_table"));
    dropTableResponse.setProperties(ImmutableMap.of("key", "value"));
    dropTableResponse.setLocation("/path/to/drop_table");
    Mockito.doReturn(dropTableResponse).when(tableOps).dropTable(any(), any());

    Response resp =
        target(String.format("/v1/table/%s/drop", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    DropTableResponse response = resp.readEntity(DropTableResponse.class);
    Assertions.assertEquals(dropTableResponse.getId(), response.getId());
    Assertions.assertEquals(dropTableResponse.getProperties(), response.getProperties());
    Assertions.assertEquals(dropTableResponse.getLocation(), response.getLocation());

    // test throw exception
    doThrow(new TableNotFoundException("Table not found", "", tableIds))
        .when(tableOps)
        .dropTable(any(), any());
    resp =
        target(String.format("/v1/table/%s/drop", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(4, errorResp.getCode());
    Assertions.assertEquals("Table not found", errorResp.getError());

    // Test runtime exception
    Mockito.reset(tableOps);
    doThrow(new RuntimeException("Runtime exception")).when(tableOps).dropTable(any(), any());
    resp =
        target(String.format("/v1/table/%s/drop", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(null);
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
  }

  @Test
  void testDropColumns() {
    String tableIds = "catalog.scheme.alter_table_drop_columns";
    String delimiter = ".";

    // first try to create a table and drop columns from it
    AlterTableDropColumnsResponse dropColumnsResponse = new AlterTableDropColumnsResponse();
    dropColumnsResponse.setVersion(2L);

    AlterTableDropColumnsRequest dropColumnsRequest = new AlterTableDropColumnsRequest();
    dropColumnsRequest.setColumns(List.of("id"));
    when(tableOps.alterTable(any(), any(), any(AlterTableDropColumnsRequest.class)))
        .thenReturn(dropColumnsResponse);
    Response resp =
        target(String.format("/v1/table/%s/drop_columns", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(dropColumnsRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    AlterTableDropColumnsResponse response = resp.readEntity(AlterTableDropColumnsResponse.class);
    Assertions.assertEquals(dropColumnsResponse.getVersion(), response.getVersion());

    // Test empty columns validation
    AlterTableDropColumnsRequest emptyColumnsRequest = new AlterTableDropColumnsRequest();
    emptyColumnsRequest.setColumns(List.of());
    resp =
        target(String.format("/v1/table/%s/drop_columns", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(emptyColumnsRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Columns to drop cannot be empty.", errorResp.getError());

    // Test blank column names validation
    AlterTableDropColumnsRequest blankColumnRequest = new AlterTableDropColumnsRequest();
    blankColumnRequest.setColumns(List.of(" "));
    resp =
        target(String.format("/v1/table/%s/drop_columns", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(blankColumnRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Columns to drop cannot be blank.", errorResp.getError());

    // Test runtime exception
    Mockito.reset(tableOps);
    when(tableOps.alterTable(any(), any(), any(AlterTableDropColumnsRequest.class)))
        .thenThrow(new RuntimeException("Runtime exception"));
    resp =
        target(String.format("/v1/table/%s/drop_columns", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(dropColumnsRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    // Test No such table exception
    Mockito.reset(tableOps);
    when(tableOps.alterTable(any(), any(), any(AlterTableDropColumnsRequest.class)))
        .thenThrow(new TableNotFoundException("Table not found", "", tableIds));
    resp =
        target(String.format("/v1/table/%s/drop_columns", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(dropColumnsRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());
  }

  @Test
  void testAlterColumns() {
    String tableIds = "catalog.scheme.alter_table_alter_columns";
    String delimiter = ".";

    AlterTableAlterColumnsResponse alterColumnsResponse = new AlterTableAlterColumnsResponse();
    alterColumnsResponse.setVersion(3L);

    AlterTableAlterColumnsRequest alterColumnsRequest = new AlterTableAlterColumnsRequest();
    alterColumnsRequest.setId(List.of("catalog", "scheme", "alter_table_alter_columns"));
    AlterColumnsEntry columnAlteration = new AlterColumnsEntry();
    columnAlteration.setPath("col1");
    columnAlteration.setRename("col1_new");
    alterColumnsRequest.setAlterations(List.of(columnAlteration));

    when(tableOps.alterTable(any(), any(), any(AlterTableAlterColumnsRequest.class)))
        .thenReturn(alterColumnsResponse);
    Response resp =
        target(String.format("/v1/table/%s/alter_columns", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(alterColumnsRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    AlterTableAlterColumnsResponse response = resp.readEntity(AlterTableAlterColumnsResponse.class);
    Assertions.assertEquals(alterColumnsResponse.getVersion(), response.getVersion());

    // Missing rename should return a clear validation message.
    AlterTableAlterColumnsRequest missingRenameRequest = new AlterTableAlterColumnsRequest();
    missingRenameRequest.setId(List.of("catalog", "scheme", "alter_table_alter_columns"));
    AlterColumnsEntry missingRenameAlteration = new AlterColumnsEntry();
    missingRenameAlteration.setPath("col1");
    missingRenameAlteration.setRename("  ");
    missingRenameRequest.setAlterations(List.of(missingRenameAlteration));
    resp =
        target(String.format("/v1/table/%s/alter_columns", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(missingRenameRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Rename field must be specified.", errorResp.getError());

    // Non-rename alteration fields should still be rejected separately.
    AlterTableAlterColumnsRequest withUnsupportedFieldRequest = new AlterTableAlterColumnsRequest();
    withUnsupportedFieldRequest.setId(List.of("catalog", "scheme", "alter_table_alter_columns"));
    AlterColumnsEntry withUnsupportedFieldAlteration = new AlterColumnsEntry();
    withUnsupportedFieldAlteration.setPath("col1");
    withUnsupportedFieldAlteration.setRename("col1_new");
    withUnsupportedFieldAlteration.setNullable(Boolean.TRUE);
    withUnsupportedFieldRequest.setAlterations(List.of(withUnsupportedFieldAlteration));
    resp =
        target(String.format("/v1/table/%s/alter_columns", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(withUnsupportedFieldRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Only RENAME alteration is supported currently.", errorResp.getError());

    Mockito.reset(tableOps);
    when(tableOps.alterTable(any(), any(), any(AlterTableAlterColumnsRequest.class)))
        .thenThrow(new RuntimeException("Runtime exception"));
    resp =
        target(String.format("/v1/table/%s/alter_columns", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(alterColumnsRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    Mockito.reset(tableOps);
    when(tableOps.alterTable(any(), any(), any(AlterTableAlterColumnsRequest.class)))
        .thenThrow(new TableNotFoundException("Table not found", "", tableIds));
    resp =
        target(String.format("/v1/table/%s/alter_columns", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(alterColumnsRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());
  }

  @Test
  void testDeclareTable() {
    String tableIds = "catalog.scheme.declare_table";
    String delimiter = ".";

    // Test normal
    DeclareTableResponse declareTableResponse = new DeclareTableResponse();
    declareTableResponse.setLocation("/path/to/table");
    declareTableResponse.setStorageOptions(ImmutableMap.of("key", "value"));
    when(tableOps.declareTable(any(), any(), any(), any())).thenReturn(declareTableResponse);

    DeclareTableRequest tableRequest = new DeclareTableRequest();
    tableRequest.setLocation("/path/to/table");

    Response resp =
        target(String.format("/v1/table/%s/declare", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    DeclareTableResponse response = resp.readEntity(DeclareTableResponse.class);
    Assertions.assertEquals(declareTableResponse.getLocation(), response.getLocation());
    Assertions.assertEquals(declareTableResponse.getStorageOptions(), response.getStorageOptions());

    Mockito.verify(tableOps)
        .declareTable(eq(tableIds), eq(delimiter), eq("/path/to/table"), eq(Map.of()));

    // Test illegal argument
    Mockito.reset(tableOps);
    when(tableOps.declareTable(any(), any(), any(), any()))
        .thenThrow(new IllegalArgumentException("Illegal argument"));

    resp =
        target(String.format("/v1/table/%s/declare", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    // Test runtime exception
    Mockito.reset(tableOps);
    when(tableOps.declareTable(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Runtime exception"));
    resp =
        target(String.format("/v1/table/%s/declare", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(tableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals("Runtime exception", errorResp.getError());
  }

  @Test
  void testDescribeTableVendCredentialsDefaultTrue() {
    String tableIds = "catalog.scheme.vend_table";
    String delimiter = ".";

    DescribeTableResponse expectedResponse = new DescribeTableResponse();
    expectedResponse.setLocation("/path/to/vend_table");
    expectedResponse.setStorageOptions(
        ImmutableMap.of(
            "aws_access_key_id",
            "temp_key",
            "aws_secret_access_key",
            "temp_secret",
            "aws_session_token",
            "temp_token",
            "expires_at_millis",
            "1234567890"));
    when(tableOps.describeTable(any(), any(), any(), any())).thenReturn(expectedResponse);

    DescribeTableRequest request = new DescribeTableRequest();
    Response resp =
        target(String.format("/v1/table/%s/describe", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    DescribeTableResponse response = resp.readEntity(DescribeTableResponse.class);
    Assertions.assertNotNull(response.getStorageOptions());
    Assertions.assertTrue(response.getStorageOptions().containsKey("expires_at_millis"));

    Mockito.verify(tableOps).describeTable(any(), any(), any(), any());
  }

  @Test
  void testDescribeTableVendCredentialsExplicitFalse() {
    Mockito.reset(tableOps);
    String tableIds = "catalog.scheme.no_vend";
    String delimiter = ".";

    DescribeTableResponse expectedResponse = new DescribeTableResponse();
    expectedResponse.setLocation("/path/to/no_vend");
    expectedResponse.setStorageOptions(ImmutableMap.of("aws_region", "us-east-1"));
    when(tableOps.describeTable(any(), any(), any(), eq(null))).thenReturn(expectedResponse);

    DescribeTableRequest request = new DescribeTableRequest();
    request.setVendCredentials(false);
    Response resp =
        target(String.format("/v1/table/%s/describe", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Mockito.verify(tableOps).describeTable(any(), any(), any(), eq(null));
  }

  @Test
  void testDescribeTableVendCredentialsExplicitTrue() {
    Mockito.reset(tableOps);
    String tableIds = "catalog.scheme.explicit_vend";
    String delimiter = ".";

    DescribeTableResponse expectedResponse = new DescribeTableResponse();
    expectedResponse.setLocation("/path/to/explicit_vend");
    when(tableOps.describeTable(any(), any(), any(), any())).thenReturn(expectedResponse);

    DescribeTableRequest request = new DescribeTableRequest();
    request.setVendCredentials(true);
    Response resp =
        target(String.format("/v1/table/%s/describe", tableIds))
            .queryParam("delimiter", delimiter)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Mockito.verify(tableOps).describeTable(any(), any(), any(), any());
  }
}
