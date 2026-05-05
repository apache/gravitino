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

package org.apache.gravitino.auth.local.web.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.apache.gravitino.auth.local.IdpAuthenticationManager;
import org.apache.gravitino.auth.local.dto.requests.CreateUserRequest;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.junit.jupiter.api.Test;

public class TestIdpUserOperationsExceptionHandling {

  @Test
  public void testCreateUserMapsAlreadyExistsException() throws Exception {
    IdpAuthenticationManager authenticationManager = mock(IdpAuthenticationManager.class);
    when(authenticationManager.createUser("alice", "Passw0rd"))
        .thenThrow(new UserAlreadyExistsException("alice already exists"));
    IdpUserOperations operations = new IdpUserOperations(authenticationManager);
    setHttpRequest(operations);

    Response response = operations.addUser(new CreateUserRequest("alice", "Passw0rd"));
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertEquals(
        "Failed to operate built-in IdP user [alice] operation [ADD], reason [alice already exists]",
        errorResponse.getMessage());
  }

  @Test
  public void testGetUserMapsNotFoundException() throws Exception {
    IdpAuthenticationManager authenticationManager = mock(IdpAuthenticationManager.class);
    when(authenticationManager.getUser("missing"))
        .thenThrow(new NoSuchUserException("missing user not found"));
    IdpUserOperations operations = new IdpUserOperations(authenticationManager);
    setHttpRequest(operations);

    Response response = operations.getUser("missing");
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();

    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    assertEquals(
        "Failed to operate built-in IdP user [missing] operation [GET], reason [missing user not found]",
        errorResponse.getMessage());
  }

  private void setHttpRequest(IdpUserOperations operations) throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(anyString())).thenReturn(null);
    Field field = IdpUserOperations.class.getDeclaredField("httpRequest");
    field.setAccessible(true);
    field.set(operations, request);
  }
}
