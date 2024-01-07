/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.dto.rel.SchemaDTO;
import com.datastrato.gravitino.dto.requests.SchemaCreateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.SchemaResponse;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rest.RESTUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSchemaOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private CatalogOperationDispatcher dispatcher = mock(CatalogOperationDispatcher.class);

  private final String metalake = "metalake1";

  private final String catalog = "catalog1";

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(SchemaOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(dispatcher).to(CatalogOperationDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListSchemas() {
    NameIdentifier ident1 = NameIdentifier.of(metalake, catalog, "schema1");
    NameIdentifier ident2 = NameIdentifier.of(metalake, catalog, "schema2");

    when(dispatcher.listSchemas(any())).thenReturn(new NameIdentifier[] {ident1, ident2});

    Response resp =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    EntityListResponse listResp = resp.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, listResp.getCode());

    NameIdentifier[] idents = listResp.identifiers();
    Assertions.assertEquals(2, idents.length);
    Assertions.assertEquals(ident1, idents[0]);
    Assertions.assertEquals(ident2, idents[1]);

    // Test throw NoSuchCatalogException
    doThrow(new NoSuchCatalogException("mock error")).when(dispatcher).listSchemas(any());
    Response resp1 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchCatalogException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).listSchemas(any());
    Response resp2 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testCreateSchema() {
    SchemaCreateRequest req =
        new SchemaCreateRequest("schema1", "comment", ImmutableMap.of("key", "value"));
    Schema mockSchema = mockSchema("schema1", "comment", ImmutableMap.of("key", "value"));

    when(dispatcher.createSchema(any(), any(), any())).thenReturn(mockSchema);

    Response resp =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(javax.ws.rs.client.Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    SchemaResponse schemaResp = resp.readEntity(SchemaResponse.class);
    Assertions.assertEquals(0, schemaResp.getCode());

    SchemaDTO schemaDTO = schemaResp.getSchema();
    Assertions.assertEquals("schema1", schemaDTO.name());
    Assertions.assertEquals("comment", schemaDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("key", "value"), schemaDTO.properties());

    // Test throw NoSuchCatalogException
    doThrow(new NoSuchCatalogException("mock error"))
        .when(dispatcher)
        .createSchema(any(), any(), any());
    Response resp1 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(javax.ws.rs.client.Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchCatalogException.class.getSimpleName(), errorResp.getType());

    // Test throw SchemaAlreadyExistsException
    doThrow(new SchemaAlreadyExistsException("mock error"))
        .when(dispatcher)
        .createSchema(any(), any(), any());

    Response resp2 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(javax.ws.rs.client.Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp2.getCode());
    Assertions.assertEquals(
        SchemaAlreadyExistsException.class.getSimpleName(), errorResp2.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).createSchema(any(), any(), any());

    Response resp3 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(javax.ws.rs.client.Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp3.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp3.getType());
  }

  @Test
  public void testLoadSchema() {
    Schema mockSchema = mockSchema("schema1", "comment", ImmutableMap.of("key", "value"));
    when(dispatcher.loadSchema(any())).thenReturn(mockSchema);

    Response resp =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    SchemaResponse schemaResp = resp.readEntity(SchemaResponse.class);
    Assertions.assertEquals(0, schemaResp.getCode());

    SchemaDTO schemaDTO = schemaResp.getSchema();
    Assertions.assertEquals("schema1", schemaDTO.name());
    Assertions.assertEquals("comment", schemaDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("key", "value"), schemaDTO.properties());

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error")).when(dispatcher).loadSchema(any());
    Response resp1 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).loadSchema(any());
    Response resp2 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testAlterSchema() {
    SchemaUpdateRequest commentReq =
        new SchemaUpdateRequest.UpdateSchemaCommentRequest("new comment");
    Schema commentSchema = mockSchema("schema1", "new comment", ImmutableMap.of("key", "value"));

    SchemaUpdateRequest setReq = new SchemaUpdateRequest.SetSchemaPropertyRequest("key2", "value2");
    Schema setSchema =
        mockSchema("schema1", "comment", ImmutableMap.of("key", "value", "key2", "value2"));

    SchemaUpdateRequest removeReq = new SchemaUpdateRequest.RemoveSchemaPropertyRequest("key2");
    Schema removedSchema = mockSchema("schema1", "comment", ImmutableMap.of("key", "value"));

    // Test update comment
    when(dispatcher.alterSchema(any(), eq(commentReq.schemaChange()))).thenReturn(commentSchema);
    SchemaUpdatesRequest req1 = new SchemaUpdatesRequest(ImmutableList.of(commentReq));
    Response resp1 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(javax.ws.rs.client.Entity.entity(req1, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    SchemaResponse schemaResp1 = resp1.readEntity(SchemaResponse.class);
    Assertions.assertEquals(0, schemaResp1.getCode());

    SchemaDTO schemaDTO1 = schemaResp1.getSchema();
    Assertions.assertEquals("schema1", schemaDTO1.name());
    Assertions.assertEquals("new comment", schemaDTO1.comment());
    Assertions.assertEquals(ImmutableMap.of("key", "value"), schemaDTO1.properties());

    // Test set property
    when(dispatcher.alterSchema(any(), eq(setReq.schemaChange()))).thenReturn(setSchema);
    SchemaUpdatesRequest req2 = new SchemaUpdatesRequest(ImmutableList.of(setReq));
    Response resp2 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(javax.ws.rs.client.Entity.entity(req2, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    SchemaResponse schemaResp2 = resp2.readEntity(SchemaResponse.class);
    Assertions.assertEquals(0, schemaResp2.getCode());

    SchemaDTO schemaDTO2 = schemaResp2.getSchema();
    Assertions.assertEquals("schema1", schemaDTO2.name());
    Assertions.assertEquals("comment", schemaDTO2.comment());
    Assertions.assertEquals(
        ImmutableMap.of("key", "value", "key2", "value2"), schemaDTO2.properties());

    // Test remove property
    when(dispatcher.alterSchema(any(), eq(removeReq.schemaChange()))).thenReturn(removedSchema);
    SchemaUpdatesRequest req3 = new SchemaUpdatesRequest(ImmutableList.of(removeReq));
    Response resp3 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(javax.ws.rs.client.Entity.entity(req3, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    SchemaResponse schemaResp3 = resp3.readEntity(SchemaResponse.class);
    Assertions.assertEquals(0, schemaResp3.getCode());

    SchemaDTO schemaDTO3 = schemaResp3.getSchema();
    Assertions.assertEquals("schema1", schemaDTO3.name());
    Assertions.assertEquals("comment", schemaDTO3.comment());
    Assertions.assertEquals(ImmutableMap.of("key", "value"), schemaDTO3.properties());

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error")).when(dispatcher).alterSchema(any(), any());
    Response resp4 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(javax.ws.rs.client.Entity.entity(req1, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp4.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp4.getMediaType());

    ErrorResponse errorResp4 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp4.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp4.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).alterSchema(any(), any());
    Response resp5 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(javax.ws.rs.client.Entity.entity(req1, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp5.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp5.getMediaType());

    ErrorResponse errorResp5 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp5.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp5.getType());
  }

  @Test
  public void testDropSchema() {
    when(dispatcher.dropSchema(any(), eq(false))).thenReturn(true);

    Response resp =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    // Test when failed to drop schema
    when(dispatcher.dropSchema(any(), eq(false))).thenReturn(false);

    Response resp1 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    DropResponse dropResp1 = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp1.getCode());
    Assertions.assertFalse(dropResp1.dropped());

    // Test specifying cascade to true
    boolean cascade = true;
    when(dispatcher.dropSchema(any(), eq(false))).thenReturn(true);
    doThrow(NonEmptySchemaException.class).when(dispatcher).dropSchema(any(), eq(true));

    Response resp2 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .queryParam("cascade", cascade)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NON_EMPTY_CODE, errorResp.getCode());
    Assertions.assertEquals(NonEmptySchemaException.class.getSimpleName(), errorResp.getType());

    // Test specifying cascade to false
    Response resp3 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .queryParam("cascade", !cascade)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    DropResponse dropResp2 = resp3.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp2.getCode());
    Assertions.assertTrue(dropResp2.dropped());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).dropSchema(any(), eq(false));
    Response resp4 =
        target("/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/schema1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp4.getMediaType());

    ErrorResponse errorResp4 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp4.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp4.getType());
  }

  private static Schema mockSchema(String name, String comment, Map<String, String> properties) {
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.name()).thenReturn(name);
    when(mockSchema.comment()).thenReturn(comment);
    when(mockSchema.properties()).thenReturn(properties);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(mockSchema.auditInfo()).thenReturn(mockAudit);

    return mockSchema;
  }
}
