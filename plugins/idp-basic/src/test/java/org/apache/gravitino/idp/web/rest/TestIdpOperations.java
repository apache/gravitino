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
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.dto.requests.AddGroupRequest;
import org.apache.gravitino.idp.dto.requests.AddUserRequest;
import org.apache.gravitino.idp.dto.requests.ChangePasswordRequest;
import org.apache.gravitino.idp.dto.requests.GroupMembershipChangeRequest;
import org.apache.gravitino.idp.dto.responses.IdpGroupResponse;
import org.apache.gravitino.idp.dto.responses.IdpUserResponse;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestIdpOperations extends JerseyTest {

  private static final String ACCEPT = "application/vnd.gravitino.v1+json";
  private static final String VALID_PASSWORD = "Passw0rd-For-User";
  private static final IdpUserGroupManager MANAGER = mock(IdpUserGroupManager.class);

  @BeforeEach
  void resetManager() {
    reset(MANAGER);
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 4000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(null);

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(IdpUserOperations.class);
    resourceConfig.register(IdpGroupOperations.class);
    resourceConfig.register(new IdpAuthorizationFilter(() -> List.of("admin"), () -> "admin"));
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(MANAGER).to(IdpUserGroupManager.class);
            bind(request).to(HttpServletRequest.class);
          }
        });
    return resourceConfig;
  }

  @Test
  void testAddUser() throws Exception {
    AddUserRequest req = new AddUserRequest("user1", VALID_PASSWORD);
    doReturn(buildUser("user1")).when(MANAGER).addUser("user1", VALID_PASSWORD);

    assertError(
        Response.Status.BAD_REQUEST,
        post("/idp/users", new AddUserRequest("", VALID_PASSWORD)),
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE);

    Assertions.assertEquals(
        "user1", post("/idp/users", req).readEntity(IdpUserResponse.class).getUser().name());

    doThrow(new AlreadyExistsException("mock error"))
        .when(MANAGER)
        .addUser("user1", VALID_PASSWORD);
    assertStatus(Response.Status.CONFLICT, post("/idp/users", req));
  }

  @Test
  void testGetUser() {
    when(MANAGER.getUser("user1")).thenReturn(buildUser("user1"));
    Assertions.assertEquals(
        "user1", get("/idp/users/user1").readEntity(IdpUserResponse.class).getUser().name());

    reset(MANAGER);
    when(MANAGER.getUser("user1")).thenThrow(new NotFoundException("mock error"));
    assertStatus(Response.Status.NOT_FOUND, get("/idp/users/user1"));
  }

  @Test
  void testChangePasswordAndRemoveUser() {
    ChangePasswordRequest req = new ChangePasswordRequest(VALID_PASSWORD);
    when(MANAGER.changePassword("user1", VALID_PASSWORD)).thenReturn(true);
    when(MANAGER.getUser("user1")).thenReturn(buildUser("user1"));
    when(MANAGER.removeUser("user1")).thenReturn(true);

    Assertions.assertEquals(
        "user1", put("/idp/users/user1", req).readEntity(IdpUserResponse.class).getUser().name());
    Assertions.assertTrue(delete("/idp/users/user1").readEntity(RemoveResponse.class).removed());
  }

  @Test
  void testAddAndGetGroup() throws Exception {
    AddGroupRequest req = new AddGroupRequest("group1");
    doReturn(buildGroup("group1")).when(MANAGER).addGroup("group1");
    when(MANAGER.getGroup("group1")).thenReturn(buildGroup("group1"));

    assertStatus(Response.Status.OK, post("/idp/groups", req));
    Assertions.assertEquals(
        "group1", get("/idp/groups/group1").readEntity(IdpGroupResponse.class).getGroup().name());

    doThrow(new AlreadyExistsException("mock error")).when(MANAGER).addGroup("group1");
    assertStatus(Response.Status.CONFLICT, post("/idp/groups", req));
  }

  @Test
  void testGetGroupNotFound() {
    when(MANAGER.getGroup("group1")).thenThrow(new NotFoundException("mock error"));
    assertError(
        Response.Status.NOT_FOUND, get("/idp/groups/group1"), ErrorConstants.NOT_FOUND_CODE);
  }

  @Test
  void testAddAndRemoveGroupUsers() {
    GroupMembershipChangeRequest addReq =
        new GroupMembershipChangeRequest(new String[] {"user1", "user2"}, null);
    when(MANAGER.changeGroupMembership("group1", Arrays.asList("user1", "user2"), null))
        .thenReturn(buildGroup("group1", Arrays.asList("user1", "user2")));
    assertStatus(Response.Status.OK, put("/idp/groups/group1/users", addReq));

    GroupMembershipChangeRequest removeReq =
        new GroupMembershipChangeRequest(null, new String[] {"user1", "user2"});
    when(MANAGER.changeGroupMembership("group1", null, Arrays.asList("user1", "user2")))
        .thenReturn(buildGroup("group1"));
    assertStatus(Response.Status.OK, put("/idp/groups/group1/users", removeReq));
  }

  @Test
  void testRemoveGroup() {
    when(MANAGER.removeGroup("group1", true)).thenReturn(true);
    Assertions.assertTrue(
        target("/idp/groups/group1")
            .queryParam("force", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(ACCEPT)
            .delete()
            .readEntity(RemoveResponse.class)
            .removed());
  }

  private Invocation.Builder request(String path) {
    return target(path).request(MediaType.APPLICATION_JSON_TYPE).accept(ACCEPT);
  }

  private Response get(String path) {
    return request(path).get();
  }

  private Response post(String path, Object body) {
    return request(path).post(Entity.entity(body, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response put(String path, Object body) {
    return request(path).put(Entity.entity(body, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response delete(String path) {
    return request(path).delete();
  }

  private void assertStatus(Response.Status expected, Response response) {
    Assertions.assertEquals(expected.getStatusCode(), response.getStatus());
  }

  private void assertError(Response.Status expected, Response response, int code) {
    assertStatus(expected, response);
    Assertions.assertEquals(code, response.readEntity(ErrorResponse.class).getCode());
  }

  private IdpUser buildUser(String user) {
    return new IdpUser(user, Collections.emptyList());
  }

  private IdpGroup buildGroup(String group) {
    return buildGroup(group, Collections.emptyList());
  }

  private IdpGroup buildGroup(String group, List<String> users) {
    return new IdpGroup(group, users);
  }
}
