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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.dto.authorization.UserDTO;
import org.apache.gravitino.dto.requests.UserAddRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.dto.responses.UserListResponse;
import org.apache.gravitino.dto.responses.UserResponse;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestUserOperations extends BaseOperationsTest {

  private static final AccessControlManager manager = mock(AccessControlManager.class);
  private static final OwnerDispatcher ownerDispatcher = mock(OwnerDispatcher.class);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "accessControlDispatcher", manager, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", ownerDispatcher, true);
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
    resourceConfig.register(UserOperations.class);
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
  public void testAddUser() {
    UserAddRequest req = new UserAddRequest("user1");
    User user = buildUser("user1");

    when(manager.addUser(any(), any())).thenReturn(user);

    // test with IllegalRequest
    UserAddRequest illegalReq = new UserAddRequest("");
    Response illegalResp =
        target("/metalakes/metalake1/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(illegalReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());

    Response resp =
        target("/metalakes/metalake1/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    UserResponse userResponse = resp.readEntity(UserResponse.class);
    Assertions.assertEquals(0, userResponse.getCode());

    UserDTO userDTO = userResponse.getUser();
    Assertions.assertEquals("user1", userDTO.name());
    Assertions.assertNotNull(userDTO.roles());
    Assertions.assertTrue(userDTO.roles().isEmpty());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).addUser(any(), any());
    Response resp1 =
        target("/metalakes/metalake1/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw UserAlreadyExistsException
    doThrow(new UserAlreadyExistsException("mock error")).when(manager).addUser(any(), any());
    Response resp2 =
        target("/metalakes/metalake1/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse1.getCode());
    Assertions.assertEquals(
        UserAlreadyExistsException.class.getSimpleName(), errorResponse1.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).addUser(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/users")
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
  public void testGetUser() {

    User user = buildUser("user1");

    when(manager.getUser(any(), any())).thenReturn(user);

    Response resp =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    UserResponse userResponse = resp.readEntity(UserResponse.class);
    Assertions.assertEquals(0, userResponse.getCode());
    UserDTO userDTO = userResponse.getUser();
    Assertions.assertEquals("user1", userDTO.name());
    Assertions.assertNotNull(userDTO.roles());
    Assertions.assertTrue(userDTO.roles().isEmpty());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).getUser(any(), any());
    Response resp1 =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw NoSuchUserException
    doThrow(new NoSuchUserException("mock error")).when(manager).getUser(any(), any());
    Response resp2 =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse1.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse1.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).getUser(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  private User buildUser(String user) {
    return UserEntity.builder()
        .withId(1L)
        .withName(user)
        .withRoleNames(Collections.emptyList())
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  @Test
  public void testRemoveUser() {
    // Delete the metalake owner
    when(manager.removeUser(any(), any())).thenReturn(true);
    Owner owner = mock(Owner.class);
    when(owner.type()).thenReturn(Owner.Type.USER);
    when(owner.name()).thenReturn("user1");
    when(ownerDispatcher.getOwner(any(), any())).thenReturn(Optional.of(owner));

    Response respOwner =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), respOwner.getStatus());

    ErrorResponse errorResponseOwner = respOwner.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponseOwner.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), errorResponseOwner.getType());

    // Delete user normally
    when(owner.name()).thenReturn("user2");
    Response resp =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    RemoveResponse removeResponse = resp.readEntity(RemoveResponse.class);
    Assertions.assertEquals(0, removeResponse.getCode());
    Assertions.assertTrue(removeResponse.removed());

    // Test when failed to remove user
    when(manager.removeUser(any(), any())).thenReturn(false);
    Response resp2 =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    RemoveResponse removeResponse2 = resp2.readEntity(RemoveResponse.class);
    Assertions.assertEquals(0, removeResponse2.getCode());
    Assertions.assertFalse(removeResponse2.removed());

    doThrow(new RuntimeException("mock error")).when(manager).removeUser(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testListUsernames() {
    when(manager.listUserNames(any())).thenReturn(new String[] {"user"});

    Response resp =
        target("/metalakes/metalake1/users/")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    NameListResponse listResponse = resp.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, listResponse.getCode());

    Assertions.assertEquals(1, listResponse.getNames().length);
    Assertions.assertEquals("user", listResponse.getNames()[0]);

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).listUserNames(any());
    Response resp1 =
        target("/metalakes/metalake1/users/")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).listUserNames(any());
    Response resp3 =
        target("/metalakes/metalake1/users")
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
  public void testListUsers() {
    User user = buildUser("user");
    when(manager.listUsers(any())).thenReturn(new User[] {user});

    Response resp =
        target("/metalakes/metalake1/users/")
            .queryParam("details", "true")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    UserListResponse listResponse = resp.readEntity(UserListResponse.class);
    Assertions.assertEquals(0, listResponse.getCode());

    Assertions.assertEquals(1, listResponse.getUsers().length);
    Assertions.assertEquals(user.name(), listResponse.getUsers()[0].name());
    Assertions.assertEquals(user.roles(), listResponse.getUsers()[0].roles());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).listUsers(any());
    Response resp1 =
        target("/metalakes/metalake1/users/")
            .queryParam("details", "true")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).listUsers(any());
    Response resp3 =
        target("/metalakes/metalake1/users")
            .queryParam("details", "true")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }
}
