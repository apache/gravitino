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

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.requests.MetalakeCreateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.MetalakeManager;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.rest.RESTUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
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
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestMetalakeOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private MetalakeManager metalakeManager = mock(MetalakeManager.class);

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
    resourceConfig.register(MetalakeOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(metalakeManager).to(MetalakeManager.class).ranked(2);
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
    AuditInfo info = AuditInfo.builder().withCreator("gravitino").withCreateTime(now).build();
    BaseMetalake metalake =
        new BaseMetalake.Builder()
            .withName(metalakeName)
            .withId(id)
            .withAuditInfo(info)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    when(metalakeManager.listMetalakes()).thenReturn(new BaseMetalake[] {metalake, metalake});

    Response resp =
        target("/metalakes")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    MetalakeListResponse metalakeListResponse = resp.readEntity(MetalakeListResponse.class);
    Assertions.assertEquals(0, metalakeListResponse.getCode());

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
            .withAuditInfo(AuditInfo.builder().withCreator("gravitino").withCreateTime(now).build())
            .withVersion(SchemaVersion.V_0_1)
            .build();

    when(metalakeManager.createMetalake(any(), any(), any())).thenReturn(mockMetalake);

    Response resp =
        target("/metalakes")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    MetalakeResponse metalakeResponse = resp.readEntity(MetalakeResponse.class);
    Assertions.assertEquals(0, metalakeResponse.getCode());

    MetalakeDTO metalake = metalakeResponse.getMetalake();
    Assertions.assertEquals("metalake", metalake.name());
    Assertions.assertEquals("comment", metalake.comment());
    Assertions.assertEquals(ImmutableMap.of("k1", "v1"), metalake.properties());

    MetalakeCreateRequest req1 = new MetalakeCreateRequest(null, null, null);
    Response resp1 =
        target("/metalakes")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req1, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testLoadMetalake() {
    String metalakeName = "test";
    Long id = 1L;
    Instant now = Instant.now();
    AuditInfo info = AuditInfo.builder().withCreator("gravitino").withCreateTime(now).build();
    BaseMetalake metalake =
        new BaseMetalake.Builder()
            .withName(metalakeName)
            .withId(id)
            .withAuditInfo(info)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    when(metalakeManager.loadMetalake(any())).thenReturn(metalake);

    Response resp =
        target("/metalakes/" + metalakeName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    MetalakeResponse metalakeResponse = resp.readEntity(MetalakeResponse.class);
    Assertions.assertEquals(0, metalakeResponse.getCode());

    MetalakeDTO metalake1 = metalakeResponse.getMetalake();
    Assertions.assertEquals(metalakeName, metalake1.name());
    Assertions.assertNull(metalake1.comment());
    Assertions.assertNull(metalake1.properties());

    // Test when specified metalake is not found.
    doThrow(new NoSuchMetalakeException("Failed to find metalake by name %s", metalakeName))
        .when(metalakeManager)
        .loadMetalake(any());

    Response resp1 =
        target("/metalakes/" + metalakeName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());
    Assertions.assertTrue(
        errorResponse
            .getMessage()
            .contains("Failed to operate metalake(s) [" + metalakeName + "] operation [LOAD]"));

    // Test with internal error
    doThrow(new RuntimeException("Internal error")).when(metalakeManager).loadMetalake(any());

    Response resp2 =
        target("/metalakes/" + metalakeName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
    Assertions.assertTrue(
        errorResponse1
            .getMessage()
            .contains("Failed to operate object [" + metalakeName + "] operation [LOAD]"));
  }

  @Test
  public void testAlterMetalake() {
    String metalakeName = "test";
    Long id = 1L;
    Instant now = Instant.now();
    AuditInfo info = AuditInfo.builder().withCreator("gravitino").withCreateTime(now).build();
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

    when(metalakeManager.alterMetalake(any(), any(), any())).thenReturn(metalake);

    MetalakeUpdatesRequest req = new MetalakeUpdatesRequest(updateRequests);

    Response resp =
        target("/metalakes/" + metalakeName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    MetalakeResponse metalakeResponse = resp.readEntity(MetalakeResponse.class);
    Assertions.assertEquals(0, metalakeResponse.getCode());

    MetalakeDTO metalake1 = metalakeResponse.getMetalake();
    Assertions.assertEquals(metalakeName, metalake1.name());
    Assertions.assertNull(metalake1.comment());
    Assertions.assertNull(metalake1.properties());

    // Test when specified metalake is not found.
    doThrow(new NoSuchMetalakeException("Failed to find metalake by name %s", metalakeName))
        .when(metalakeManager)
        .alterMetalake(any(), any(), any());

    Response resp1 =
        target("/metalakes/" + metalakeName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test with internal error
    doThrow(new RuntimeException("Internal error"))
        .when(metalakeManager)
        .alterMetalake(any(), any(), any());

    Response resp2 =
        target("/metalakes/" + metalakeName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testDropMetalake() {
    when(metalakeManager.dropMetalake(any())).thenReturn(true);
    Response resp =
        target("/metalakes/test")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    boolean dropped = dropResponse.dropped();
    Assertions.assertTrue(dropped);

    // Test throw an exception when deleting tenant.
    doThrow(new RuntimeException("Internal error")).when(metalakeManager).dropMetalake(any());

    Response resp1 =
        target("/metalakes/test")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
    Assertions.assertTrue(
        errorResponse.getMessage().contains("Failed to operate object [test] operation [DROP]"));
  }
}
