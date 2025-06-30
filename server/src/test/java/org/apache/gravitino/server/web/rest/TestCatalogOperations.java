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
package org.apache.gravitino.server.web.rest;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static org.apache.gravitino.Catalog.PROPERTY_IN_USE;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.CatalogSetRequest;
import org.apache.gravitino.dto.requests.CatalogUpdateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdatesRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.CatalogListResponse;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCatalogOperations extends BaseOperationsTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private CatalogManager manager = mock(CatalogManager.class);

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(CatalogOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(manager).to(CatalogDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListCatalogs() {
    NameIdentifier ident1 = NameIdentifier.of("metalake1", "catalog1");
    NameIdentifier ident2 = NameIdentifier.of("metalake1", "catalog2");

    when(manager.listCatalogs(any())).thenReturn(new NameIdentifier[] {ident1, ident2});

    Response resp =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    EntityListResponse listResponse = resp.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, listResponse.getCode());

    NameIdentifier[] idents = listResponse.identifiers();
    Assertions.assertEquals(2, idents.length);
    Assertions.assertEquals(ident1, idents[0]);
    Assertions.assertEquals(ident2, idents[1]);

    doThrow(new NoSuchMetalakeException("mock error")).when(manager).listCatalogs(any());
    Response resp1 =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testListCatalogsInfo() {
    TestCatalog catalog1 = buildCatalog("metalake1", "catalog1");
    TestCatalog catalog2 = buildCatalog("metalake1", "catalog2");

    when(manager.listCatalogsInfo(any())).thenReturn(new Catalog[] {catalog1, catalog2});

    Response resp =
        target("/metalakes/metalake1/catalogs")
            .queryParam("details", "true")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    CatalogListResponse catalogResponse = resp.readEntity(CatalogListResponse.class);
    Assertions.assertEquals(0, catalogResponse.getCode());

    CatalogDTO[] catalogDTOs = catalogResponse.getCatalogs();
    Assertions.assertEquals(2, catalogDTOs.length);

    CatalogDTO catalogDTO1 = catalogDTOs[0];
    Assertions.assertEquals("catalog1", catalogDTO1.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogDTO1.type());
    Assertions.assertEquals("comment", catalogDTO1.comment());
    Assertions.assertEquals(
        ImmutableMap.of("key", "value", PROPERTY_IN_USE, "true"), catalogDTO1.properties());

    CatalogDTO catalogDTO2 = catalogDTOs[1];
    Assertions.assertEquals("catalog2", catalogDTO2.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogDTO2.type());
    Assertions.assertEquals("comment", catalogDTO2.comment());
    Assertions.assertEquals(
        ImmutableMap.of("key", "value", PROPERTY_IN_USE, "true"), catalogDTO2.properties());

    doThrow(new NoSuchMetalakeException("mock error")).when(manager).listCatalogsInfo(any());
    Response resp1 =
        target("/metalakes/metalake1/catalogs")
            .queryParam("details", "true")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testCreateCatalog() {
    CatalogCreateRequest req =
        new CatalogCreateRequest(
            "catalog1",
            Catalog.Type.RELATIONAL,
            "test",
            "comment",
            ImmutableMap.of("key", "value"));
    TestCatalog catalog = buildCatalog("metalake1", "catalog1");

    when(manager.createCatalog(any(), any(), any(), any(), any())).thenReturn(catalog);

    Response resp =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    CatalogResponse catalogResponse = resp.readEntity(CatalogResponse.class);
    Assertions.assertEquals(0, catalogResponse.getCode());

    CatalogDTO catalogDTO = catalogResponse.getCatalog();
    Assertions.assertEquals("catalog1", catalogDTO.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogDTO.type());
    Assertions.assertEquals("comment", catalogDTO.comment());
    Assertions.assertEquals(
        ImmutableMap.of("key", "value", PROPERTY_IN_USE, "true"), catalogDTO.properties());

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(manager)
        .createCatalog(any(), any(), any(), any(), any());
    Response resp1 =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test throw CatalogAlreadyExistsException
    doThrow(new CatalogAlreadyExistsException("mock error"))
        .when(manager)
        .createCatalog(any(), any(), any(), any(), any());
    Response resp2 =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse1.getCode());
    Assertions.assertEquals(
        CatalogAlreadyExistsException.class.getSimpleName(), errorResponse1.getType());

    // Test throw internal RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .createCatalog(any(), any(), any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  @Test
  public void testConnection() {
    CatalogCreateRequest req =
        new CatalogCreateRequest(
            "catalog1",
            Catalog.Type.RELATIONAL,
            "test",
            "comment",
            ImmutableMap.of("key", "value"));
    doNothing().when(manager).testConnection(any(), any(), any(), any(), any());
    Response resp =
        target("/metalakes/metalake1/catalogs/testConnection")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    BaseResponse testResponse = resp.readEntity(BaseResponse.class);
    Assertions.assertEquals(0, testResponse.getCode());

    // test throw RuntimeException
    doThrow(new RuntimeException("connection failed"))
        .when(manager)
        .testConnection(any(), any(), any(), any(), any());
    Response resp1 =
        target("/metalakes/metalake1/catalogs/testConnection")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testLoadCatalog() {
    TestCatalog catalog = buildCatalog("metalake1", "catalog1");

    when(manager.loadCatalog(any())).thenReturn(catalog);

    Response resp =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    CatalogResponse catalogResponse = resp.readEntity(CatalogResponse.class);
    Assertions.assertEquals(0, catalogResponse.getCode());

    CatalogDTO catalogDTO = catalogResponse.getCatalog();
    Assertions.assertEquals("catalog1", catalogDTO.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogDTO.type());
    Assertions.assertEquals("comment", catalogDTO.comment());
    Assertions.assertEquals(
        ImmutableMap.of("key", "value", PROPERTY_IN_USE, "true"), catalogDTO.properties());

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).loadCatalog(any());
    Response resp1 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test throw NoSuchCatalogException
    doThrow(new NoSuchCatalogException("mock error")).when(manager).loadCatalog(any());
    Response resp2 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse1.getCode());
    Assertions.assertEquals(NoSuchCatalogException.class.getSimpleName(), errorResponse1.getType());

    // Test throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).loadCatalog(any());
    Response resp3 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  @Test
  public void testAlterCatalog() {
    TestCatalog catalog = buildCatalog("metalake1", "catalog2");

    when(manager.alterCatalog(any(), any())).thenReturn(catalog);

    CatalogUpdateRequest updateRequest = new CatalogUpdateRequest.RenameCatalogRequest("catalog2");
    CatalogUpdatesRequest req = new CatalogUpdatesRequest(ImmutableList.of(updateRequest));

    Response resp =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    CatalogResponse catalogResponse = resp.readEntity(CatalogResponse.class);
    Assertions.assertEquals(0, catalogResponse.getCode());

    CatalogDTO catalogDTO = catalogResponse.getCatalog();
    Assertions.assertEquals("catalog2", catalogDTO.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogDTO.type());
    Assertions.assertEquals("comment", catalogDTO.comment());
    Assertions.assertEquals(
        ImmutableMap.of("key", "value", PROPERTY_IN_USE, "true"), catalogDTO.properties());

    // Test throw NoSuchCatalogException
    doThrow(new NoSuchCatalogException("mock error")).when(manager).alterCatalog(any(), any());
    Response resp2 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchCatalogException.class.getSimpleName(), errorResponse.getType());

    // Test throw IllegalArgumentException
    doThrow(new IllegalArgumentException("mock error")).when(manager).alterCatalog(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse1 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponse1.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), errorResponse1.getType());

    // Test throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).alterCatalog(any(), any());
    Response resp4 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResponse2 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  @Test
  public void testDropCatalog() {
    when(manager.dropCatalog(any(), anyBoolean())).thenReturn(true);

    Response resp =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    Assertions.assertTrue(dropResponse.dropped());

    // Test catalog does not exist
    when(manager.dropCatalog(any(), anyBoolean())).thenReturn(false);

    Response resp2 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    DropResponse dropResponse2 = resp2.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse2.getCode());
    Assertions.assertFalse(dropResponse2.dropped());

    // Test throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).dropCatalog(any(), anyBoolean());
    Response resp3 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testSetCatalog() {
    CatalogSetRequest req = new CatalogSetRequest(true);
    doNothing().when(manager).enableCatalog(any());

    Response resp =
        target("/metalakes/metalake1/catalogs/catalog1")
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .method("PATCH", Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BaseResponse baseResponse = resp.readEntity(BaseResponse.class);
    Assertions.assertEquals(0, baseResponse.getCode());

    req = new CatalogSetRequest(false);
    doNothing().when(manager).disableCatalog(any());

    resp =
        target("/metalakes/metalake1/catalogs/catalog1")
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .method("PATCH", Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    baseResponse = resp.readEntity(BaseResponse.class);
    Assertions.assertEquals(0, baseResponse.getCode());
  }

  private static TestCatalog buildCatalog(String metalake, String catalogName) {
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName(catalogName)
            .withComment("comment")
            .withNamespace(Namespace.of(metalake))
            .withProperties(ImmutableMap.of("key", "value"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAuditInfo(
                AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    return new TestCatalog().withCatalogConf(Collections.emptyMap()).withCatalogEntity(entity);
  }
}
