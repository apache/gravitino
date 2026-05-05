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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.auth.local.IdpUserManager;
import org.apache.gravitino.dto.IdpUserDTO;
import org.apache.gravitino.dto.requests.CreateUserRequest;
import org.apache.gravitino.dto.requests.ResetPasswordRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.IdpUserResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIdpUserOperations {

  @Test
  public void testAddUser() throws IllegalAccessException {
    CreateUserRequest req = new CreateUserRequest("user1", "Passw0rd");
    IdpUserDTO user = buildUser("user1");
    IdpUserManager userManager = mock(IdpUserManager.class);
    IdpUserOperations operations = buildOperations(userManager);

    when(userManager.createUser("user1", "Passw0rd")).thenReturn(user);

    CreateUserRequest illegalReq = new CreateUserRequest("", "Passw0rd");
    Response illegalResp = operations.addUser(illegalReq);
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, illegalResp.getMediaType());

    ErrorResponse illegalResponse = (ErrorResponse) illegalResp.getEntity();
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, illegalResponse.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), illegalResponse.getType());

    Response resp = operations.addUser(req);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpUserResponse userResponse = (IdpUserResponse) resp.getEntity();
    Assertions.assertEquals(0, userResponse.getCode());
    Assertions.assertEquals("user1", userResponse.getUser().name());
    Assertions.assertNotNull(userResponse.getUser().groups());
    Assertions.assertTrue(userResponse.getUser().groups().isEmpty());

    doThrow(new UserAlreadyExistsException("mock error"))
        .when(userManager)
        .createUser("user1", "Passw0rd");
    Response resp1 = operations.addUser(req);

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = (ErrorResponse) resp1.getEntity();
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        UserAlreadyExistsException.class.getSimpleName(), errorResponse.getType());

    Mockito.reset(userManager);
    when(userManager.createUser("user1", "Passw0rd")).thenThrow(new RuntimeException("mock error"));
    Response resp2 = operations.addUser(req);

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResponse1 = (ErrorResponse) resp2.getEntity();
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testGetUser() throws IllegalAccessException {
    IdpUserDTO user = buildUser("user1");
    IdpUserManager userManager = mock(IdpUserManager.class);
    IdpUserOperations operations = buildOperations(userManager);

    when(userManager.getUser("user1")).thenReturn(user);

    Response resp = operations.getUser("user1");

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpUserResponse userResponse = (IdpUserResponse) resp.getEntity();
    Assertions.assertEquals(0, userResponse.getCode());
    Assertions.assertEquals("user1", userResponse.getUser().name());
    Assertions.assertNotNull(userResponse.getUser().groups());
    Assertions.assertTrue(userResponse.getUser().groups().isEmpty());

    doThrow(new NoSuchUserException("mock error")).when(userManager).getUser("user1");
    Response resp1 = operations.getUser("user1");

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = (ErrorResponse) resp1.getEntity();
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse.getType());

    Mockito.reset(userManager);
    when(userManager.getUser("user1")).thenThrow(new RuntimeException("mock error"));
    Response resp2 = operations.getUser("user1");

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResponse1 = (ErrorResponse) resp2.getEntity();
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testResetPassword() throws IllegalAccessException {
    ResetPasswordRequest req = new ResetPasswordRequest("Passw0rd1");
    IdpUserDTO user = buildUser("user1");
    IdpUserManager userManager = mock(IdpUserManager.class);
    IdpUserOperations operations = buildOperations(userManager);

    when(userManager.resetPassword("user1", "Passw0rd1")).thenReturn(user);

    ResetPasswordRequest illegalReq = new ResetPasswordRequest("");
    Response illegalResp = operations.resetPassword("user1", illegalReq);
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, illegalResp.getMediaType());

    ErrorResponse illegalResponse = (ErrorResponse) illegalResp.getEntity();
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, illegalResponse.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), illegalResponse.getType());

    Response resp = operations.resetPassword("user1", req);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpUserResponse userResponse = (IdpUserResponse) resp.getEntity();
    Assertions.assertEquals(0, userResponse.getCode());
    Assertions.assertEquals("user1", userResponse.getUser().name());
    Assertions.assertNotNull(userResponse.getUser().groups());
    Assertions.assertTrue(userResponse.getUser().groups().isEmpty());

    doThrow(new NoSuchUserException("mock error"))
        .when(userManager)
        .resetPassword("user1", "Passw0rd1");
    Response resp1 = operations.resetPassword("user1", req);

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = (ErrorResponse) resp1.getEntity();
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse.getType());

    Mockito.reset(userManager);
    when(userManager.resetPassword("user1", "Passw0rd1"))
        .thenThrow(new RuntimeException("mock error"));
    Response resp2 = operations.resetPassword("user1", req);

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResponse1 = (ErrorResponse) resp2.getEntity();
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testRemoveUser() throws IllegalAccessException {
    IdpUserManager userManager = mock(IdpUserManager.class);
    IdpUserOperations operations = buildOperations(userManager);

    when(userManager.deleteUser("user1")).thenReturn(true);

    Response resp = operations.removeUser("user1");

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    RemoveResponse removeResponse = (RemoveResponse) resp.getEntity();
    Assertions.assertEquals(0, removeResponse.getCode());
    Assertions.assertTrue(removeResponse.removed());

    when(userManager.deleteUser("user1")).thenReturn(false);
    Response resp1 = operations.removeUser("user1");

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    RemoveResponse removeResponse1 = (RemoveResponse) resp1.getEntity();
    Assertions.assertEquals(0, removeResponse1.getCode());
    Assertions.assertFalse(removeResponse1.removed());

    Mockito.reset(userManager);
    when(userManager.deleteUser("user1")).thenThrow(new RuntimeException("mock error"));
    Response resp2 = operations.removeUser("user1");

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResponse = (ErrorResponse) resp2.getEntity();
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }

  private IdpUserDTO buildUser(String user) {
    return IdpUserDTO.builder().withName(user).build();
  }

  private IdpUserOperations buildOperations(IdpUserManager userManager)
      throws IllegalAccessException {
    IdpUserOperations operations = new IdpUserOperations(userManager);
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(anyString())).thenReturn(null);
    FieldUtils.writeField(operations, "httpRequest", request, true);
    return operations;
  }
}
