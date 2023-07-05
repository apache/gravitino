package com.datastrato.graviton.server.web.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.dto.CatalogDTO;
import com.datastrato.graviton.dto.requests.CatalogCreateRequest;
import com.datastrato.graviton.dto.requests.CatalogUpdateRequest;
import com.datastrato.graviton.dto.requests.CatalogUpdatesRequest;
import com.datastrato.graviton.dto.responses.BaseResponse;
import com.datastrato.graviton.dto.responses.CatalogListResponse;
import com.datastrato.graviton.dto.responses.CatalogResponse;
import com.datastrato.graviton.dto.responses.ErrorType;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseCatalogsOperations;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private BaseCatalogsOperations ops = mock(BaseCatalogsOperations.class);

  @Override
  protected Application configure() {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(CatalogOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(ops).to(BaseCatalogsOperations.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListCatalogs() {
    TestCatalog catalog1 = buildCatalog("metalake1", "catalog1");
    TestCatalog catalog2 = buildCatalog("metalake1", "catalog2");

    when(ops.listCatalogs(any())).thenReturn(new Catalog[] {catalog1, catalog2});

    Response resp =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    CatalogListResponse listResponse = resp.readEntity(CatalogListResponse.class);
    Assertions.assertEquals(0, listResponse.getCode());
    Assertions.assertNull(listResponse.getType());
    Assertions.assertNull(listResponse.getMessage());

    CatalogDTO[] catalogs = listResponse.getCatalogs();
    Assertions.assertEquals(2, catalogs.length);
    Assertions.assertEquals("catalog1", catalogs[0].name());
    Assertions.assertEquals("catalog2", catalogs[1].name());

    doThrow(new NoSuchMetalakeException("mock error")).when(ops).listCatalogs(any());
    Response resp1 =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorType(), baseResponse.getType());
  }

  @Test
  public void testCreateCatalog() {
    CatalogCreateRequest req =
        new CatalogCreateRequest(
            "catalog1", Catalog.Type.RELATIONAL, "comment", ImmutableMap.of("key", "value"));
    TestCatalog catalog = buildCatalog("metalake1", "catalog1");

    when(ops.createCatalog(any(), any(), any(), any())).thenReturn(catalog);

    Response resp =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    CatalogResponse catalogResponse = resp.readEntity(CatalogResponse.class);
    Assertions.assertEquals(0, catalogResponse.getCode());
    Assertions.assertNull(catalogResponse.getType());
    Assertions.assertNull(catalogResponse.getMessage());

    CatalogDTO catalogDTO = catalogResponse.getCatalog();
    Assertions.assertEquals("catalog1", catalogDTO.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogDTO.type());
    Assertions.assertEquals("comment", catalogDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("key", "value"), catalogDTO.properties());

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(ops)
        .createCatalog(any(), any(), any(), any());
    Response resp1 =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorType(), baseResponse.getType());

    // Test throw CatalogAlreadyExistsException
    doThrow(new CatalogAlreadyExistsException("mock error"))
        .when(ops)
        .createCatalog(any(), any(), any(), any());
    Response resp2 =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    BaseResponse baseResponse2 = resp2.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.ALREADY_EXISTS.errorCode(), baseResponse2.getCode());
    Assertions.assertEquals(ErrorType.ALREADY_EXISTS.errorType(), baseResponse2.getType());

    // Test throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(ops).createCatalog(any(), any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/catalogs")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    BaseResponse baseResponse3 = resp3.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorCode(), baseResponse3.getCode());
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorType(), baseResponse3.getType());
  }

  @Test
  public void testLoadCatalog() {
    TestCatalog catalog = buildCatalog("metalake1", "catalog1");

    when(ops.loadCatalog(any())).thenReturn(catalog);

    Response resp =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    CatalogResponse catalogResponse = resp.readEntity(CatalogResponse.class);
    Assertions.assertEquals(0, catalogResponse.getCode());
    Assertions.assertNull(catalogResponse.getType());
    Assertions.assertNull(catalogResponse.getMessage());

    CatalogDTO catalogDTO = catalogResponse.getCatalog();
    Assertions.assertEquals("catalog1", catalogDTO.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogDTO.type());
    Assertions.assertEquals("comment", catalogDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("key", "value"), catalogDTO.properties());

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(ops).loadCatalog(any());
    Response resp1 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorType(), baseResponse.getType());

    // Test throw NoSuchCatalogException
    doThrow(new NoSuchCatalogException("mock error")).when(ops).loadCatalog(any());
    Response resp2 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    BaseResponse baseResponse2 = resp2.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorCode(), baseResponse2.getCode());
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorType(), baseResponse2.getType());

    // Test throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(ops).loadCatalog(any());
    Response resp3 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    BaseResponse baseResponse3 = resp3.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorCode(), baseResponse3.getCode());
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorType(), baseResponse3.getType());
  }

  @Test
  public void testAlterCatalog() {
    TestCatalog catalog = buildCatalog("metalake1", "catalog2");

    when(ops.alterCatalog(any(), any())).thenReturn(catalog);

    CatalogUpdateRequest updateRequest = new CatalogUpdateRequest.RenameCatalogRequest("catalog2");
    CatalogUpdatesRequest req = new CatalogUpdatesRequest(ImmutableList.of(updateRequest));

    Response resp =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    CatalogResponse catalogResponse = resp.readEntity(CatalogResponse.class);
    Assertions.assertEquals(0, catalogResponse.getCode());
    Assertions.assertNull(catalogResponse.getType());
    Assertions.assertNull(catalogResponse.getMessage());

    CatalogDTO catalogDTO = catalogResponse.getCatalog();
    Assertions.assertEquals("catalog2", catalogDTO.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogDTO.type());
    Assertions.assertEquals("comment", catalogDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("key", "value"), catalogDTO.properties());

    // Test throw NoSuchCatalogException
    doThrow(new NoSuchCatalogException("mock error")).when(ops).alterCatalog(any(), any());
    Response resp2 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    BaseResponse baseResponse = resp2.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorType(), baseResponse.getType());

    // Test throw IllegalArgumentException
    doThrow(new IllegalArgumentException("mock error")).when(ops).alterCatalog(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp3.getStatus());

    BaseResponse baseResponse1 = resp3.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INVALID_ARGUMENTS.errorCode(), baseResponse1.getCode());
    Assertions.assertEquals(ErrorType.INVALID_ARGUMENTS.errorType(), baseResponse1.getType());

    // Test throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(ops).alterCatalog(any(), any());
    Response resp4 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());

    BaseResponse baseResponse2 = resp4.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorCode(), baseResponse2.getCode());
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorType(), baseResponse2.getType());
  }

  @Test
  public void testDropCatalog() {
    when(ops.dropCatalog(any())).thenReturn(true);

    Response resp =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), resp.getStatus());

    // Test when failed to drop catalog
    when(ops.dropCatalog(any())).thenReturn(false);

    Response resp2 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    BaseResponse baseResponse = resp2.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorType(), baseResponse.getType());

    // Test throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(ops).dropCatalog(any());
    Response resp3 =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    BaseResponse baseResponse1 = resp3.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorCode(), baseResponse1.getCode());
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorType(), baseResponse1.getType());
  }

  private static TestCatalog buildCatalog(String metalake, String catalogName) {
    return new TestCatalog.Builder()
        .withId(1L)
        .withMetalakeId(1L)
        .withName(catalogName)
        .withComment("comment")
        .withNamespace(Namespace.of(metalake))
        .withProperties(ImmutableMap.of("key", "value"))
        .withType(Catalog.Type.RELATIONAL)
        .withAuditInfo(
            new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }
}
