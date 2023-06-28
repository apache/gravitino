package com.datastrato.graviton.server.web.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datastrato.graviton.LakehouseChange;
import com.datastrato.graviton.dto.LakehouseDTO;
import com.datastrato.graviton.dto.requests.LakehouseCreateRequest;
import com.datastrato.graviton.dto.requests.LakehouseUpdateRequest;
import com.datastrato.graviton.dto.requests.LakehouseUpdatesRequest;
import com.datastrato.graviton.dto.responses.BaseResponse;
import com.datastrato.graviton.dto.responses.ErrorType;
import com.datastrato.graviton.dto.responses.LakehouseResponse;
import com.datastrato.graviton.exceptions.NoSuchLakehouseException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseLakehouse;
import com.datastrato.graviton.meta.BaseLakehousesOperations;
import com.datastrato.graviton.meta.SchemaVersion;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
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

public class TestLakehousesOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private BaseLakehousesOperations lakehousesOperations = mock(BaseLakehousesOperations.class);

  @Override
  protected Application configure() {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(LakehouseOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(lakehousesOperations).to(BaseLakehousesOperations.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testCreateLakehouse() {
    LakehouseCreateRequest req =
        new LakehouseCreateRequest("lakehouse", "comment", ImmutableMap.of("k1", "v1"));
    Instant now = Instant.now();

    BaseLakehouse mockLakehouse =
        new BaseLakehouse.Builder()
            .withId(1L)
            .withName("lakehouse")
            .withComment("comment")
            .withProperties(ImmutableMap.of("k1", "v1"))
            .withAuditInfo(
                new AuditInfo.Builder().withCreator("graviton").withCreateTime(now).build())
            .withVersion(SchemaVersion.V_0_1)
            .build();

    when(lakehousesOperations.createLakehouse(any(), any(), any())).thenReturn(mockLakehouse);

    Response resp =
        target("/lakehouses")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    LakehouseResponse lakehouseResponse = resp.readEntity(LakehouseResponse.class);
    Assertions.assertEquals(0, lakehouseResponse.getCode());
    Assertions.assertNull(lakehouseResponse.getMessage());
    Assertions.assertNull(lakehouseResponse.getType());

    LakehouseDTO lakehouse = lakehouseResponse.getLakehouse();
    Assertions.assertEquals("lakehouse", lakehouse.name());
    Assertions.assertEquals("comment", lakehouse.comment());
    Assertions.assertEquals(ImmutableMap.of("k1", "v1"), lakehouse.properties());

    LakehouseCreateRequest req1 = new LakehouseCreateRequest(null, null, null);
    Response resp1 =
        target("/lakehouses")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req1, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp1.getStatus());

    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INVALID_ARGUMENTS.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.INVALID_ARGUMENTS.errorType(), baseResponse.getType());
    Assertions.assertEquals(
        "\"name\" field is required and cannot be empty", baseResponse.getMessage());
  }

  @Test
  public void testGetLakehouse() {
    String lakehouseName = "test";
    Long id = 1L;
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator("graviton").withCreateTime(now).build();
    BaseLakehouse lakehouse =
        new BaseLakehouse.Builder()
            .withName(lakehouseName)
            .withId(id)
            .withAuditInfo(info)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    when(lakehousesOperations.loadLakehouse(any())).thenReturn(lakehouse);

    Response resp =
        target("/lakehouses/" + lakehouseName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    LakehouseResponse lakehouseResponse = resp.readEntity(LakehouseResponse.class);
    Assertions.assertEquals(0, lakehouseResponse.getCode());
    Assertions.assertNull(lakehouseResponse.getMessage());
    Assertions.assertNull(lakehouseResponse.getType());

    LakehouseDTO lakehouse1 = lakehouseResponse.getLakehouse();
    Assertions.assertEquals(lakehouseName, lakehouse1.name());
    Assertions.assertNull(lakehouse1.comment());
    Assertions.assertNull(lakehouse1.properties());

    // Test when specified lakehouse is not found.
    doThrow(new NoSuchLakehouseException("Failed to find lakehouse by name " + lakehouseName))
        .when(lakehousesOperations)
        .loadLakehouse(any());

    Response resp1 =
        target("/lakehouses/" + lakehouseName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorType(), baseResponse.getType());
    Assertions.assertEquals(
        "Failed to find lakehouse by name " + lakehouseName, baseResponse.getMessage());

    // Test with internal error
    doThrow(new RuntimeException("Internal error")).when(lakehousesOperations).loadLakehouse(any());

    Response resp2 =
        target("/lakehouses/" + lakehouseName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    BaseResponse baseResponse1 = resp2.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorCode(), baseResponse1.getCode());
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorType(), baseResponse1.getType());
    Assertions.assertEquals("Internal error", baseResponse1.getMessage());
  }

  @Test
  public void testUpdateLakehouse() {
    String lakehouseName = "test";
    Long id = 1L;
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator("graviton").withCreateTime(now).build();
    BaseLakehouse lakehouse =
        new BaseLakehouse.Builder()
            .withName(lakehouseName)
            .withId(id)
            .withAuditInfo(info)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    List<LakehouseUpdateRequest> updateRequests =
        Lists.newArrayList(
            new LakehouseUpdateRequest.RenameLakehouseRequest("newTest"),
            new LakehouseUpdateRequest.UpdateLakehouseCommentRequest("newComment"));
    LakehouseChange[] changes =
        updateRequests.stream()
            .map(LakehouseUpdateRequest::lakehouseChange)
            .toArray(LakehouseChange[]::new);

    when(lakehousesOperations.alterLakehouse(any(), any(), any())).thenReturn(lakehouse);

    LakehouseUpdatesRequest req = new LakehouseUpdatesRequest(updateRequests);

    Response resp =
        target("/lakehouses/" + lakehouseName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    LakehouseResponse lakehouseResponse = resp.readEntity(LakehouseResponse.class);
    Assertions.assertEquals(0, lakehouseResponse.getCode());
    Assertions.assertNull(lakehouseResponse.getMessage());
    Assertions.assertNull(lakehouseResponse.getType());

    LakehouseDTO lakehouse1 = lakehouseResponse.getLakehouse();
    Assertions.assertEquals(lakehouseName, lakehouse1.name());
    Assertions.assertNull(lakehouse1.comment());
    Assertions.assertNull(lakehouse1.properties());

    // Test when specified lakehouse is not found.
    doThrow(new NoSuchLakehouseException("Failed to find lakehouse by name " + lakehouseName))
        .when(lakehousesOperations)
        .alterLakehouse(any(), any(), any());

    Response resp1 =
        target("/lakehouses/" + lakehouseName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorType(), baseResponse.getType());

    // Test with internal error
    doThrow(new RuntimeException("Internal error"))
        .when(lakehousesOperations)
        .alterLakehouse(any(), any(), any());

    Response resp2 =
        target("/lakehouses/" + lakehouseName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    BaseResponse baseResponse1 = resp2.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorCode(), baseResponse1.getCode());
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorType(), baseResponse1.getType());
  }

  @Test
  public void testDeleteLakehouse() {
    when(lakehousesOperations.dropLakehouse(any())).thenReturn(true);
    Response resp =
        target("/lakehouses/test")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Test throw an exception when deleting tenant.
    doThrow(new RuntimeException("Internal error")).when(lakehousesOperations).dropLakehouse(any());

    Response resp1 =
        target("/lakehouses/test")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp1.getStatus());

    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorType(), baseResponse.getType());
    Assertions.assertEquals("Internal error", baseResponse.getMessage());
  }
}
