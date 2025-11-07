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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.lancedb.lance.namespace.model.CreateNamespaceRequest;
import com.lancedb.lance.namespace.model.CreateNamespaceResponse;
import com.lancedb.lance.namespace.model.DescribeNamespaceResponse;
import com.lancedb.lance.namespace.model.DropNamespaceRequest;
import com.lancedb.lance.namespace.model.DropNamespaceResponse;
import com.lancedb.lance.namespace.model.ErrorResponse;
import com.lancedb.lance.namespace.model.ListNamespacesResponse;
import java.io.IOException;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
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

public class TestLanceNamespaceOperations extends JerseyTest {

  private static NamespaceWrapper namespaceWrapper = mock(NamespaceWrapper.class);
  private static org.apache.gravitino.lance.common.ops.LanceNamespaceOperations namespaceOps =
      mock(org.apache.gravitino.lance.common.ops.LanceNamespaceOperations.class);

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
    Assertions.assertEquals(500, errorResp.getCode());
    Assertions.assertEquals("Test exception", errorResp.getError());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
    Assertions.assertEquals("ns1.ns2", errorResp.getInstance());
    Assertions.assertNotNull(errorResp.getDetail());
    Assertions.assertTrue(errorResp.getDetail().contains("Test exception"));
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
    Assertions.assertEquals(500, errorResp.getCode());
    Assertions.assertEquals("Test exception", errorResp.getError());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
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
            eq(CreateNamespaceRequest.ModeEnum.CREATE),
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
    Assertions.assertEquals(500, errorResp.getCode());
    Assertions.assertEquals("Test exception", errorResp.getError());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
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
    Assertions.assertEquals(404, errorResp.getCode());
    Assertions.assertEquals("Not found", errorResp.getError());
    Assertions.assertEquals(NoSuchCatalogException.class.getSimpleName(), errorResp.getType());
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
        .dropNamespace(
            eq(namespaceId),
            eq(Pattern.quote(delimiter)),
            eq(DropNamespaceRequest.ModeEnum.FAIL),
            eq(DropNamespaceRequest.BehaviorEnum.RESTRICT));
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
    Assertions.assertEquals(500, errorResp.getCode());
    Assertions.assertEquals("Test exception", errorResp.getError());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
  }
}
