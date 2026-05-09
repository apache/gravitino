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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.authorization.IdpGroupManager;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.IdpGroupDTO;
import org.apache.gravitino.dto.requests.CreateGroupRequest;
import org.apache.gravitino.dto.requests.UpdateGroupUsersRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.IdpGroupResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIdpGroupOperations extends BaseOperationsTest {

  private static final IdpGroupManager MANAGER = mock(IdpGroupManager.class);

  public static class TestableIdpGroupOperations extends IdpGroupOperations {
    public TestableIdpGroupOperations() {
      super(MANAGER);
    }
  }

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  @BeforeEach
  public void resetManager() {
    reset(MANAGER);
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
    resourceConfig.register(TestableIdpGroupOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testAddGroup() {
    CreateGroupRequest req = new CreateGroupRequest("group1");
    IdpGroupDTO group = buildGroup("group1");

    when(MANAGER.createGroup("group1")).thenReturn(group);

    // test with IllegalRequest
    CreateGroupRequest illegalReq = new CreateGroupRequest("");
    Response illegalResp =
        target("/idp/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(illegalReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, illegalResp.getMediaType());

    ErrorResponse illegalResponse = illegalResp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, illegalResponse.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), illegalResponse.getType());

    Response resp =
        target("/idp/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpGroupResponse groupResponse = resp.readEntity(IdpGroupResponse.class);
    Assertions.assertEquals(0, groupResponse.getCode());
    Assertions.assertEquals("group1", groupResponse.getGroup().name());
    Assertions.assertNotNull(groupResponse.getGroup().users());
    Assertions.assertTrue(groupResponse.getGroup().users().isEmpty());
    Assertions.assertEquals("admin", groupResponse.getGroup().auditInfo().creator());

    // Test to throw GroupAlreadyExistsException
    doThrow(new GroupAlreadyExistsException("mock error")).when(MANAGER).createGroup("group1");
    Response resp1 =
        target("/idp/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        GroupAlreadyExistsException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    reset(MANAGER);
    when(MANAGER.createGroup("group1")).thenThrow(new RuntimeException("mock error"));
    Response resp2 =
        target("/idp/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testGetGroup() {
    IdpGroupDTO group = buildGroup("group1");

    when(MANAGER.getGroup("group1")).thenReturn(group);

    Response resp =
        target("/idp/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpGroupResponse groupResponse = resp.readEntity(IdpGroupResponse.class);
    Assertions.assertEquals(0, groupResponse.getCode());
    Assertions.assertEquals("group1", groupResponse.getGroup().name());
    Assertions.assertNotNull(groupResponse.getGroup().users());
    Assertions.assertTrue(groupResponse.getGroup().users().isEmpty());

    // Test to throw NoSuchGroupException
    doThrow(new NoSuchGroupException("mock error")).when(MANAGER).getGroup("group1");
    Response resp1 =
        target("/idp/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchGroupException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    reset(MANAGER);
    when(MANAGER.getGroup("group1")).thenThrow(new RuntimeException("mock error"));
    Response resp2 =
        target("/idp/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testRemoveGroup() {
    when(MANAGER.deleteGroup("group1", false)).thenReturn(true);

    Response resp =
        target("/idp/groups/group1")
            .queryParam("force", false)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    RemoveResponse removeResponse = resp.readEntity(RemoveResponse.class);
    Assertions.assertEquals(0, removeResponse.getCode());
    Assertions.assertTrue(removeResponse.removed());

    // Test when failed to remove group
    when(MANAGER.deleteGroup("group1", false)).thenReturn(false);
    Response resp1 =
        target("/idp/groups/group1")
            .queryParam("force", false)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    RemoveResponse removeResponse1 = resp1.readEntity(RemoveResponse.class);
    Assertions.assertEquals(0, removeResponse1.getCode());
    Assertions.assertFalse(removeResponse1.removed());

    // Test to throw internal RuntimeException
    reset(MANAGER);
    when(MANAGER.deleteGroup("group1", false)).thenThrow(new RuntimeException("mock error"));
    Response resp2 =
        target("/idp/groups/group1")
            .queryParam("force", false)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testAddUsers() {
    UpdateGroupUsersRequest req = new UpdateGroupUsersRequest(Collections.singletonList("user1"));
    IdpGroupDTO group = buildGroup("group1");

    when(MANAGER.addUsersToGroup("group1", Collections.singletonList("user1"))).thenReturn(group);

    // test with IllegalRequest
    UpdateGroupUsersRequest illegalReq = new UpdateGroupUsersRequest(Collections.emptyList());
    Response illegalResp =
        target("/idp/groups/group1/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(illegalReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, illegalResp.getMediaType());

    ErrorResponse illegalResponse = illegalResp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, illegalResponse.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), illegalResponse.getType());

    Response resp =
        target("/idp/groups/group1/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpGroupResponse groupResponse = resp.readEntity(IdpGroupResponse.class);
    Assertions.assertEquals(0, groupResponse.getCode());
    Assertions.assertEquals("group1", groupResponse.getGroup().name());
    Assertions.assertNotNull(groupResponse.getGroup().users());
    Assertions.assertTrue(groupResponse.getGroup().users().isEmpty());

    // Test to throw NoSuchUserException
    doThrow(new NoSuchUserException("mock error"))
        .when(MANAGER)
        .addUsersToGroup("group1", Collections.singletonList("user1"));
    Response resp1 =
        target("/idp/groups/group1/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    reset(MANAGER);
    when(MANAGER.addUsersToGroup("group1", Collections.singletonList("user1")))
        .thenThrow(new RuntimeException("mock error"));
    Response resp2 =
        target("/idp/groups/group1/add")
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
  public void testRemoveUsers() {
    UpdateGroupUsersRequest req = new UpdateGroupUsersRequest(Collections.singletonList("user1"));
    IdpGroupDTO group = buildGroup("group1");

    when(MANAGER.removeUsersFromGroup("group1", Collections.singletonList("user1")))
        .thenReturn(group);

    // test with IllegalRequest
    UpdateGroupUsersRequest illegalReq = new UpdateGroupUsersRequest(Collections.emptyList());
    Response illegalResp =
        target("/idp/groups/group1/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(illegalReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, illegalResp.getMediaType());

    ErrorResponse illegalResponse = illegalResp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, illegalResponse.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), illegalResponse.getType());

    Response resp =
        target("/idp/groups/group1/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpGroupResponse groupResponse = resp.readEntity(IdpGroupResponse.class);
    Assertions.assertEquals(0, groupResponse.getCode());
    Assertions.assertEquals("group1", groupResponse.getGroup().name());
    Assertions.assertNotNull(groupResponse.getGroup().users());
    Assertions.assertTrue(groupResponse.getGroup().users().isEmpty());

    // Test to throw NoSuchGroupException
    doThrow(new NoSuchGroupException("mock error"))
        .when(MANAGER)
        .removeUsersFromGroup("group1", Collections.singletonList("user1"));
    Response resp1 =
        target("/idp/groups/group1/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchGroupException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    reset(MANAGER);
    when(MANAGER.removeUsersFromGroup("group1", Collections.singletonList("user1")))
        .thenThrow(new RuntimeException("mock error"));
    Response resp2 =
        target("/idp/groups/group1/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  private IdpGroupDTO buildGroup(String group) {
    return IdpGroupDTO.builder().withName(group).withAudit(buildAudit()).build();
  }

  private AuditDTO buildAudit() {
    return AuditDTO.builder()
        .withCreator("admin")
        .withCreateTime(Instant.parse("2024-01-01T00:00:00Z"))
        .withLastModifier("admin")
        .withLastModifiedTime(Instant.parse("2024-01-01T00:00:00Z"))
        .build();
  }
}
