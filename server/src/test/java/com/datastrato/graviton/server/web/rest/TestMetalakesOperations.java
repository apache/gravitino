package com.datastrato.graviton.server.web.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.requests.MetalakeCreateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.graviton.dto.responses.BaseResponse;
import com.datastrato.graviton.dto.responses.ErrorType;
import com.datastrato.graviton.dto.responses.MetalakeListResponse;
import com.datastrato.graviton.dto.responses.MetalakeResponse;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.BaseMetalakesOperations;
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

public class TestMetalakesOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private BaseMetalakesOperations metalakesOperations = mock(BaseMetalakesOperations.class);

  @Override
  protected Application configure() {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(MetalakeOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(metalakesOperations).to(BaseMetalakesOperations.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListMetalakes() {
    String metalakeName = "test";
    Long id = 1L;
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator("graviton").withCreateTime(now).build();
    BaseMetalake metalake =
        new BaseMetalake.Builder()
            .withName(metalakeName)
            .withId(id)
            .withAuditInfo(info)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    when(metalakesOperations.listMetalakes()).thenReturn(new BaseMetalake[] {metalake, metalake});

    Response resp =
        target("/metalakes")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    MetalakeListResponse metalakeListResponse = resp.readEntity(MetalakeListResponse.class);
    Assertions.assertEquals(0, metalakeListResponse.getCode());
    Assertions.assertNull(metalakeListResponse.getMessage());
    Assertions.assertNull(metalakeListResponse.getType());

    MetalakeDTO[] metalakes = metalakeListResponse.getMetalakes();
    Assertions.assertEquals(2, metalakes.length);
    Assertions.assertEquals(metalakeName, metalakes[0].name());
    Assertions.assertEquals(metalakeName, metalakes[1].name());
  }

  @Test
  public void testCreateMetalake() {
    MetalakeCreateRequest req =
        new MetalakeCreateRequest("metalake", "comment", ImmutableMap.of("k1", "v1"));
    Instant now = Instant.now();

    BaseMetalake mockMetalake =
        new BaseMetalake.Builder()
            .withId(1L)
            .withName("metalake")
            .withComment("comment")
            .withProperties(ImmutableMap.of("k1", "v1"))
            .withAuditInfo(
                new AuditInfo.Builder().withCreator("graviton").withCreateTime(now).build())
            .withVersion(SchemaVersion.V_0_1)
            .build();

    when(metalakesOperations.createMetalake(any(), any(), any())).thenReturn(mockMetalake);

    Response resp =
        target("/metalakes")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    MetalakeResponse metalakeResponse = resp.readEntity(MetalakeResponse.class);
    Assertions.assertEquals(0, metalakeResponse.getCode());
    Assertions.assertNull(metalakeResponse.getMessage());
    Assertions.assertNull(metalakeResponse.getType());

    MetalakeDTO metalake = metalakeResponse.getMetalake();
    Assertions.assertEquals("metalake", metalake.name());
    Assertions.assertEquals("comment", metalake.comment());
    Assertions.assertEquals(ImmutableMap.of("k1", "v1"), metalake.properties());

    MetalakeCreateRequest req1 = new MetalakeCreateRequest(null, null, null);
    Response resp1 =
        target("/metalakes")
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
  public void testLoadMetalake() {
    String metalakeName = "test";
    Long id = 1L;
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator("graviton").withCreateTime(now).build();
    BaseMetalake metalake =
        new BaseMetalake.Builder()
            .withName(metalakeName)
            .withId(id)
            .withAuditInfo(info)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    when(metalakesOperations.loadMetalake(any())).thenReturn(metalake);

    Response resp =
        target("/metalakes/" + metalakeName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    MetalakeResponse metalakeResponse = resp.readEntity(MetalakeResponse.class);
    Assertions.assertEquals(0, metalakeResponse.getCode());
    Assertions.assertNull(metalakeResponse.getMessage());
    Assertions.assertNull(metalakeResponse.getType());

    MetalakeDTO metalake1 = metalakeResponse.getMetalake();
    Assertions.assertEquals(metalakeName, metalake1.name());
    Assertions.assertNull(metalake1.comment());
    Assertions.assertNull(metalake1.properties());

    // Test when specified metalake is not found.
    doThrow(new NoSuchMetalakeException("Failed to find metalake by name " + metalakeName))
        .when(metalakesOperations)
        .loadMetalake(any());

    Response resp1 =
        target("/metalakes/" + metalakeName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorType(), baseResponse.getType());
    Assertions.assertEquals(
        "Failed to find metalake by name " + metalakeName, baseResponse.getMessage());

    // Test with internal error
    doThrow(new RuntimeException("Internal error")).when(metalakesOperations).loadMetalake(any());

    Response resp2 =
        target("/metalakes/" + metalakeName)
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
  public void testAlterMetalake() {
    String metalakeName = "test";
    Long id = 1L;
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator("graviton").withCreateTime(now).build();
    BaseMetalake metalake =
        new BaseMetalake.Builder()
            .withName(metalakeName)
            .withId(id)
            .withAuditInfo(info)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    List<MetalakeUpdateRequest> updateRequests =
        Lists.newArrayList(
            new MetalakeUpdateRequest.RenameMetalakeRequest("newTest"),
            new MetalakeUpdateRequest.UpdateMetalakeCommentRequest("newComment"));
    MetalakeChange[] changes =
        updateRequests.stream()
            .map(MetalakeUpdateRequest::metalakeChange)
            .toArray(MetalakeChange[]::new);

    when(metalakesOperations.alterMetalake(any(), any(), any())).thenReturn(metalake);

    MetalakeUpdatesRequest req = new MetalakeUpdatesRequest(updateRequests);

    Response resp =
        target("/metalakes/" + metalakeName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    MetalakeResponse metalakeResponse = resp.readEntity(MetalakeResponse.class);
    Assertions.assertEquals(0, metalakeResponse.getCode());
    Assertions.assertNull(metalakeResponse.getMessage());
    Assertions.assertNull(metalakeResponse.getType());

    MetalakeDTO metalake1 = metalakeResponse.getMetalake();
    Assertions.assertEquals(metalakeName, metalake1.name());
    Assertions.assertNull(metalake1.comment());
    Assertions.assertNull(metalake1.properties());

    // Test when specified metalake is not found.
    doThrow(new NoSuchMetalakeException("Failed to find metalake by name " + metalakeName))
        .when(metalakesOperations)
        .alterMetalake(any(), any(), any());

    Response resp1 =
        target("/metalakes/" + metalakeName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorType(), baseResponse.getType());

    // Test with internal error
    doThrow(new RuntimeException("Internal error"))
        .when(metalakesOperations)
        .alterMetalake(any(), any(), any());

    Response resp2 =
        target("/metalakes/" + metalakeName)
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
  public void testDropMetalake() {
    when(metalakesOperations.dropMetalake(any())).thenReturn(true);
    Response resp =
        target("/metalakes/test")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), resp.getStatus());

    // Test throw an exception when deleting tenant.
    doThrow(new RuntimeException("Internal error")).when(metalakesOperations).dropMetalake(any());

    Response resp1 =
        target("/metalakes/test")
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
