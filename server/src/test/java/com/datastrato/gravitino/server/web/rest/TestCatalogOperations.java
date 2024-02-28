/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.CatalogManager;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.requests.CatalogCreateRequest;
import com.datastrato.gravitino.dto.requests.CatalogUpdateRequest;
import com.datastrato.gravitino.dto.requests.CatalogUpdatesRequest;
import com.datastrato.gravitino.dto.responses.CatalogResponse;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rest.RESTUtils;
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
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCatalogOperations extends JerseyTest {

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
  public static void setup() {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    GravitinoEnv.getInstance().setLockManager(new LockManager(config));
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
            bind(manager).to(CatalogManager.class).ranked(2);
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
    Assertions.assertEquals(ImmutableMap.of("key", "value"), catalogDTO.properties());

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

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
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
    Assertions.assertEquals(ImmutableMap.of("key", "value"), catalogDTO.properties());

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

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

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
    Assertions.assertEquals(ImmutableMap.of("key", "value"), catalogDTO.properties());

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

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResponse2 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  @Test
  public void testDropCatalog() {
    when(manager.dropCatalog(any())).thenReturn(true);

    Response resp =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    Assertions.assertTrue(dropResponse.dropped());

    // Test when failed to drop catalog
    when(manager.dropCatalog(any())).thenReturn(false);

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
    doThrow(new RuntimeException("mock error")).when(manager).dropCatalog(any());
    Response resp3 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
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
