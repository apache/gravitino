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
package com.apache.gravitino.server.web.rest;

import static com.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;

import com.apache.gravitino.Config;
import com.apache.gravitino.GravitinoEnv;
import com.apache.gravitino.authorization.AccessControlManager;
import com.apache.gravitino.authorization.User;
import com.apache.gravitino.dto.authorization.UserDTO;
import com.apache.gravitino.dto.requests.UserAddRequest;
import com.apache.gravitino.dto.responses.ErrorConstants;
import com.apache.gravitino.dto.responses.ErrorResponse;
import com.apache.gravitino.dto.responses.RemoveResponse;
import com.apache.gravitino.dto.responses.UserResponse;
import com.apache.gravitino.exceptions.UserAlreadyExistsException;
import com.apache.gravitino.lock.LockManager;
import com.apache.gravitino.meta.AuditInfo;
import com.apache.gravitino.meta.UserEntity;
import com.apache.gravitino.rest.RESTUtils;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestMetalakeAdminOperations extends JerseyTest {

  private static final AccessControlManager manager = Mockito.mock(AccessControlManager.class);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "accessControlManager", manager, true);
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
    resourceConfig.register(MetalakeAdminOperations.class);
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
  public void testAddMetalakeAdmin() {
    UserAddRequest req = new UserAddRequest("user1");
    User user = buildUser("user1");

    Mockito.when(manager.addMetalakeAdmin(any())).thenReturn(user);

    Response resp =
        target("/admins")
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

    // Test to throw UserAlreadyExistsException
    Mockito.doThrow(new UserAlreadyExistsException("mock error"))
        .when(manager)
        .addMetalakeAdmin(any());
    Response resp2 =
        target("/admins")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse1.getCode());
    Assertions.assertEquals(
        UserAlreadyExistsException.class.getSimpleName(), errorResponse1.getType());

    // Test to throw internal RuntimeException
    Mockito.doThrow(new RuntimeException("mock error")).when(manager).addMetalakeAdmin(any());
    Response resp3 =
        target("/admins")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

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
  public void testRemoveMetalakeAdmin() {
    Mockito.when(manager.removeMetalakeAdmin(any())).thenReturn(true);

    Response resp =
        target("/admins/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    RemoveResponse removeResponse = resp.readEntity(RemoveResponse.class);
    Assertions.assertEquals(0, removeResponse.getCode());
    Assertions.assertTrue(removeResponse.removed());

    // Test when failed to remove user
    Mockito.when(manager.removeMetalakeAdmin(any())).thenReturn(false);
    Response resp2 =
        target("/admins/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    RemoveResponse removeResponse2 = resp2.readEntity(RemoveResponse.class);
    Assertions.assertEquals(0, removeResponse2.getCode());
    Assertions.assertFalse(removeResponse2.removed());

    Mockito.doThrow(new RuntimeException("mock error")).when(manager).removeMetalakeAdmin(any());
    Response resp3 =
        target("/admins/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }
}
