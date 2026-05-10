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
import java.lang.reflect.Method;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.idp.basic.authorization.BasicIdpManager;
import org.apache.gravitino.idp.basic.dto.IdpUserDTO;
import org.apache.gravitino.idp.basic.dto.requests.CreateUserRequest;
import org.apache.gravitino.idp.basic.dto.requests.ResetPasswordRequest;
import org.apache.gravitino.idp.basic.dto.responses.IdpUserResponse;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestIdpUserOperations extends BaseIdpOperationsTest {
  private static final BasicIdpManager MANAGER = mock(BasicIdpManager.class);

  @BeforeEach
  void resetManager() {
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

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(null);

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(new IdpUserOperations(MANAGER));
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
  void testAddUser() {
    CreateUserRequest req = new CreateUserRequest("user1", "Passw0rd-For-User");
    IdpUserDTO user = buildUser("user1");
    when(MANAGER.createIdpUser("user1", "Passw0rd-For-User")).thenReturn(user);

    CreateUserRequest illegalReq = new CreateUserRequest("", "Passw0rd-For-User");
    Response illegalResp =
        target("/idp/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(illegalReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());

    ErrorResponse illegalResponse = illegalResp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, illegalResponse.getCode());

    Response resp =
        target("/idp/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    IdpUserResponse userResponse = resp.readEntity(IdpUserResponse.class);
    Assertions.assertEquals("user1", userResponse.getUser().name());

    doThrow(new UserAlreadyExistsException("mock error"))
        .when(MANAGER)
        .createIdpUser("user1", "Passw0rd-For-User");
    Response conflictResp =
        target("/idp/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), conflictResp.getStatus());
  }

  @Test
  void testAddUserWithNullRequest() {
    Response resp =
        target("/idp/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    ErrorResponse errorResponse = resp.readEntity(ErrorResponse.class);
    Assertions.assertTrue(errorResponse.getMessage().contains("Request body cannot be null"));
  }

  @Test
  void testGetUser() {
    when(MANAGER.getIdpUser("user1")).thenReturn(buildUser("user1"));

    Response resp =
        target("/idp/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    IdpUserResponse userResponse = resp.readEntity(IdpUserResponse.class);
    Assertions.assertEquals("user1", userResponse.getUser().name());

    reset(MANAGER);
    when(MANAGER.getIdpUser("user1")).thenThrow(new NoSuchUserException("mock error"));
    Response notFoundResp =
        target("/idp/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), notFoundResp.getStatus());
  }

  @Test
  void testResetPasswordAndRemoveUser() {
    ResetPasswordRequest req = new ResetPasswordRequest("Passw0rd-For-User");
    when(MANAGER.resetIdpUserPassword("user1", "Passw0rd-For-User")).thenReturn(buildUser("user1"));
    when(MANAGER.deleteUser("user1")).thenReturn(true);

    Response resetResp =
        target("/idp/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resetResp.getStatus());

    Response removeResp =
        target("/idp/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), removeResp.getStatus());

    RemoveResponse removeResponse = removeResp.readEntity(RemoveResponse.class);
    Assertions.assertTrue(removeResponse.removed());
  }

  @Test
  void testServiceAdminAuthorizationAnnotations() throws Exception {
    Assertions.assertTrue(
        IdpUserOperations.class.isAnnotationPresent(NameBindings.AccessControlInterfaces.class));

    Method getUserMethod = IdpUserOperations.class.getMethod("getUser", String.class);
    AuthorizationExpression getUserAuthorization =
        getUserMethod.getAnnotation(AuthorizationExpression.class);
    Assertions.assertNotNull(getUserAuthorization);
    Assertions.assertEquals("SERVICE_ADMIN", getUserAuthorization.expression());

    assertServiceAdminAuthorization(
        IdpUserOperations.class.getMethod("addUser", CreateUserRequest.class));
    assertServiceAdminAuthorization(
        IdpUserOperations.class.getMethod(
            "resetPassword", String.class, ResetPasswordRequest.class));
    assertServiceAdminAuthorization(IdpUserOperations.class.getMethod("removeUser", String.class));
  }

  private IdpUserDTO buildUser(String user) {
    return IdpUserDTO.builder().withName(user).build();
  }

  private void assertServiceAdminAuthorization(Method method) {
    AuthorizationExpression authorizationExpression =
        method.getAnnotation(AuthorizationExpression.class);
    Assertions.assertNotNull(authorizationExpression);
    Assertions.assertEquals("SERVICE_ADMIN", authorizationExpression.expression());
    Assertions.assertTrue(authorizationExpression.errorMessage().contains("service admins"));
  }
}
