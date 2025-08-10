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

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.requests.MetalakeCreateRequest;
import org.apache.gravitino.dto.requests.MetalakeUpdateRequest;
import org.apache.gravitino.dto.requests.MetalakeUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.MetalakeListResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.mapper.JsonMappingExceptionMapper;
import org.apache.gravitino.server.web.mapper.JsonParseExceptionMapper;
import org.apache.gravitino.server.web.mapper.JsonProcessingExceptionMapper;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestMetalakeOperations extends BaseOperationsTest {

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
    resourceConfig.register(MetalakeOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(metalakeManager).to(MetalakeDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });
    resourceConfig.register(JsonProcessingExceptionMapper.class);
    resourceConfig.register(JsonParseExceptionMapper.class);
    resourceConfig.register(JsonMappingExceptionMapper.class);
    resourceConfig.register(TestException.class);
    return resourceConfig;
  }

  @Path("/test")
  public static class TestException {
    @GET
    @Path("/jsonProcessingException")
    public Response getJsonProcessingException() throws JsonProcessingException {
      throw new JsonProcessingException("Error processing JSON") {};
    }

    @GET
    @Path("/jsonMappingException")
    public Response getJsonMappingException() throws JsonMappingException {
      JsonParser mockParser = Mockito.mock(JsonParser.class);
      throw JsonMappingException.from(mockParser, "Error mapping JSON");
    }

    @GET
    @Path("/jsonParseException")
    public Response getJsonParseException() throws JsonParseException {
      throw new JsonParseException("Error parsing JSON");
    }
  }

  @Test
  public void testListMetalakes() {
    String metalakeName = "test";
    Long id = 1L;
    Instant now = Instant.now();
    AuditInfo info = AuditInfo.builder().withCreator("gravitino").withCreateTime(now).build();
    BaseMetalake metalake =
        BaseMetalake.builder()
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
        BaseMetalake.builder()
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
        BaseMetalake.builder()
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
        BaseMetalake.builder()
            .withName(metalakeName)
            .withId(id)
            .withAuditInfo(info)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    List<MetalakeUpdateRequest> updateRequests =
        Lists.newArrayList(
            new MetalakeUpdateRequest.RenameMetalakeRequest("newTest"),
            new MetalakeUpdateRequest.UpdateMetalakeCommentRequest("newComment"));

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
    when(metalakeManager.dropMetalake(any(), anyBoolean())).thenReturn(true);
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

    // Test when failed to drop metalake
    when(metalakeManager.dropMetalake(any(), anyBoolean())).thenReturn(false);
    Response resp2 =
        target("/metalakes/test")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    DropResponse dropResponse2 = resp2.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse2.getCode());
    Assertions.assertFalse(dropResponse2.dropped());

    // Test throw an exception when deleting tenant.
    doThrow(new RuntimeException("Internal error"))
        .when(metalakeManager)
        .dropMetalake(any(), anyBoolean());

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

  @Test
  public void testExceptionMapper() {
    Response resp = target("/test/jsonProcessingException").request().get();
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp.getStatus());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
    Assertions.assertTrue(
        errorResp.getMessage().contains("Unexpected error occurs when json processing."));

    Response resp1 = target("/test/jsonMappingException").request().get();
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp1.getStatus());
    ErrorResponse errorResp1 = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp1.getCode());
    Assertions.assertEquals(IllegalArgumentException.class.getSimpleName(), errorResp1.getType());
    Assertions.assertTrue(errorResp1.getMessage().contains("Malformed json request"));

    Response resp2 = target("/test/jsonParseException").request().get();
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp2.getStatus());
    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp2.getCode());
    Assertions.assertEquals(IllegalArgumentException.class.getSimpleName(), errorResp2.getType());
    Assertions.assertTrue(errorResp2.getMessage().contains("Malformed json request"));
  }
}
