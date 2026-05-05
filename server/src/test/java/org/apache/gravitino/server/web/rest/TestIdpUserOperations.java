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
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.authorization.IdpUserManager;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.IdpUserDTO;
import org.apache.gravitino.dto.requests.CreateUserRequest;
import org.apache.gravitino.dto.requests.ResetPasswordRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.IdpUserResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIdpUserOperations extends BaseOperationsTest {

  private static final IdpUserManager MANAGER = mock(IdpUserManager.class);

  public static class TestableIdpUserOperations extends IdpUserOperations {
    public TestableIdpUserOperations() {
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
    resourceConfig.register(TestableIdpUserOperations.class);
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
    CreateUserRequest req = new CreateUserRequest("user1", "Passw0rd");
    IdpUserDTO user = buildUser("user1");

    when(MANAGER.createUser("user1", "Passw0rd")).thenReturn(user);

    // test with IllegalRequest
    CreateUserRequest illegalReq = new CreateUserRequest("", "Passw0rd");
    Response illegalResp =
        target("/idp/users")
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
        target("/idp/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpUserResponse userResponse = resp.readEntity(IdpUserResponse.class);
    Assertions.assertEquals(0, userResponse.getCode());
    Assertions.assertEquals("user1", userResponse.getUser().name());
    Assertions.assertNotNull(userResponse.getUser().groups());
    Assertions.assertTrue(userResponse.getUser().groups().isEmpty());
    Assertions.assertEquals("admin", userResponse.getUser().auditInfo().creator());

    // Test to throw UserAlreadyExistsException
    doThrow(new UserAlreadyExistsException("mock error"))
        .when(MANAGER)
        .createUser("user1", "Passw0rd");
    Response resp1 =
        target("/idp/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        UserAlreadyExistsException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    reset(MANAGER);
    when(MANAGER.createUser("user1", "Passw0rd")).thenThrow(new RuntimeException("mock error"));
    Response resp2 =
        target("/idp/users")
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
  public void testGetUser() {
    IdpUserDTO user = buildUser("user1");

    when(MANAGER.getUser("user1")).thenReturn(user);

    Response resp =
        target("/idp/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpUserResponse userResponse = resp.readEntity(IdpUserResponse.class);
    Assertions.assertEquals(0, userResponse.getCode());
    Assertions.assertEquals("user1", userResponse.getUser().name());
    Assertions.assertNotNull(userResponse.getUser().groups());
    Assertions.assertTrue(userResponse.getUser().groups().isEmpty());

    // Test to throw NoSuchUserException
    doThrow(new NoSuchUserException("mock error")).when(MANAGER).getUser("user1");
    Response resp1 =
        target("/idp/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    reset(MANAGER);
    when(MANAGER.getUser("user1")).thenThrow(new RuntimeException("mock error"));
    Response resp2 =
        target("/idp/users/user1")
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
  public void testResetPassword() {
    ResetPasswordRequest req = new ResetPasswordRequest("Passw0rd1");
    IdpUserDTO user = buildUser("user1");

    when(MANAGER.resetPassword("user1", "Passw0rd1")).thenReturn(user);

    // test with IllegalRequest
    ResetPasswordRequest illegalReq = new ResetPasswordRequest("");
    Response illegalResp =
        target("/idp/users/user1")
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
        target("/idp/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpUserResponse userResponse = resp.readEntity(IdpUserResponse.class);
    Assertions.assertEquals(0, userResponse.getCode());
    Assertions.assertEquals("user1", userResponse.getUser().name());
    Assertions.assertNotNull(userResponse.getUser().groups());
    Assertions.assertTrue(userResponse.getUser().groups().isEmpty());

    // Test to throw NoSuchUserException
    doThrow(new NoSuchUserException("mock error"))
        .when(MANAGER)
        .resetPassword("user1", "Passw0rd1");
    Response resp1 =
        target("/idp/users/user1")
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
    when(MANAGER.resetPassword("user1", "Passw0rd1")).thenThrow(new RuntimeException("mock error"));
    Response resp2 =
        target("/idp/users/user1")
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
  public void testRemoveUser() {
    when(MANAGER.deleteUser("user1")).thenReturn(true);

    Response resp =
        target("/idp/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    RemoveResponse removeResponse = resp.readEntity(RemoveResponse.class);
    Assertions.assertEquals(0, removeResponse.getCode());
    Assertions.assertTrue(removeResponse.removed());

    // Test when failed to remove user
    when(MANAGER.deleteUser("user1")).thenReturn(false);
    Response resp1 =
        target("/idp/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    RemoveResponse removeResponse1 = resp1.readEntity(RemoveResponse.class);
    Assertions.assertEquals(0, removeResponse1.getCode());
    Assertions.assertFalse(removeResponse1.removed());

    // Test to throw internal RuntimeException
    reset(MANAGER);
    when(MANAGER.deleteUser("user1")).thenThrow(new RuntimeException("mock error"));
    Response resp2 =
        target("/idp/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }

  private IdpUserDTO buildUser(String user) {
    return IdpUserDTO.builder().withName(user).withAudit(buildAudit()).build();
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
