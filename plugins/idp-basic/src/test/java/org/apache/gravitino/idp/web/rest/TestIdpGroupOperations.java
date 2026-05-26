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
package org.apache.gravitino.idp.web.rest;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.dto.requests.AddGroupRequest;
import org.apache.gravitino.idp.dto.requests.GroupMembershipChangeRequest;
import org.apache.gravitino.idp.dto.responses.IdpGroupResponse;
import org.apache.gravitino.idp.exception.AlreadyExistsException;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestIdpGroupOperations extends BaseIdpOperationsTest {
  private static final IdpUserGroupManager MANAGER = mock(IdpUserGroupManager.class);

  @BeforeEach
  void resetManager() {
    reset(MANAGER);
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(3001, 4000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(null);

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(new IdpGroupOperations(MANAGER));
    registerPermissiveIdpAuthorizationFilter(resourceConfig);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(request).to(HttpServletRequest.class);
          }
        });
    return resourceConfig;
  }

  @Test
  void testAddAndGetGroup() throws Exception {
    AddGroupRequest req = new AddGroupRequest("group1");
    doReturn(buildGroup("group1")).when(MANAGER).addGroup("group1");
    when(MANAGER.getGroup("group1")).thenReturn(buildGroup("group1"));

    Response addResp =
        target("/idp/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), addResp.getStatus());

    Response getResp =
        target("/idp/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), getResp.getStatus());

    IdpGroupResponse groupResponse = getResp.readEntity(IdpGroupResponse.class);
    Assertions.assertEquals("group1", groupResponse.getGroup().name());

    doThrow(new AlreadyExistsException("mock error")).when(MANAGER).addGroup("group1");
    Response conflictResp =
        target("/idp/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), conflictResp.getStatus());
  }

  @Test
  void testGetGroupNotFound() {
    when(MANAGER.getGroup("group1")).thenThrow(new NotFoundException("mock error"));

    Response resp =
        target("/idp/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());
    ErrorResponse errorResponse = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
  }

  @Test
  void testChangeMembership() {
    GroupMembershipChangeRequest addReq =
        new GroupMembershipChangeRequest(new String[] {"user1", "user2"}, null);
    when(MANAGER.changeGroupMembership("group1", Arrays.asList("user1", "user2"), null))
        .thenReturn(buildGroup("group1", Arrays.asList("user1", "user2")));

    Response addResp =
        target("/idp/groups/group1/membership")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(addReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), addResp.getStatus());

    GroupMembershipChangeRequest removeReq =
        new GroupMembershipChangeRequest(null, new String[] {"user1", "user2"});
    when(MANAGER.changeGroupMembership("group1", null, Arrays.asList("user1", "user2")))
        .thenReturn(buildGroup("group1"));

    Response removeResp =
        target("/idp/groups/group1/membership")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(removeReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), removeResp.getStatus());
  }

  @Test
  void testRemoveGroup() {
    when(MANAGER.removeGroup("group1", true)).thenReturn(true);

    Response removeResp =
        target("/idp/groups/group1")
            .queryParam("force", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), removeResp.getStatus());

    RemoveResponse removeResponse = removeResp.readEntity(RemoveResponse.class);
    Assertions.assertTrue(removeResponse.removed());
  }

  private IdpGroup buildGroup(String group) {
    return buildGroup(group, Collections.emptyList());
  }

  private IdpGroup buildGroup(String group, List<String> users) {
    return new IdpGroup(group, users);
  }
}
