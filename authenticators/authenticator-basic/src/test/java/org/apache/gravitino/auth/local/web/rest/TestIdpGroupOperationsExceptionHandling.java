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
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.apache.gravitino.auth.local.IdpAuthenticationManager;
import org.apache.gravitino.auth.local.dto.requests.UpdateGroupUsersRequest;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.junit.jupiter.api.Test;

public class TestIdpGroupOperationsExceptionHandling {

  @Test
  public void testAddUsersMapsNotFoundException() throws Exception {
    IdpAuthenticationManager authenticationManager = mock(IdpAuthenticationManager.class);
    when(authenticationManager.addUsersToGroup("dev", Collections.singletonList("alice")))
        .thenThrow(new NoSuchUserException("alice user not found"));
    IdpGroupOperations operations = new IdpGroupOperations(authenticationManager);
    setHttpRequest(operations);

    Response response =
        operations.addUsers("dev", new UpdateGroupUsersRequest(Collections.singletonList("alice")));
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();

    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    assertEquals(
        "Failed to operate built-in IdP group [dev] operation [ADD], reason [alice user not found]",
        errorResponse.getMessage());
  }

  @Test
  public void testGetGroupMapsUnsupportedOperationException() throws Exception {
    IdpAuthenticationManager authenticationManager = mock(IdpAuthenticationManager.class);
    when(authenticationManager.getGroup("dev"))
        .thenThrow(new UnsupportedOperationException("group lookup is disabled"));
    IdpGroupOperations operations = new IdpGroupOperations(authenticationManager);
    setHttpRequest(operations);

    Response response = operations.getGroup("dev");
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();

    assertEquals(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), response.getStatus());
    assertEquals(
        "Failed to operate built-in IdP group [dev] operation [GET], reason [group lookup is disabled]",
        errorResponse.getMessage());
  }

  private void setHttpRequest(IdpGroupOperations operations) throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(anyString())).thenReturn(null);
    Field field = IdpGroupOperations.class.getDeclaredField("httpRequest");
    field.setAccessible(true);
    field.set(operations, request);
  }
}
