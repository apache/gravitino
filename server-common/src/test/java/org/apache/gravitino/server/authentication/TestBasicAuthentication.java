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

package org.apache.gravitino.server.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Vector;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.auth.BasicAuthenticator;
import org.apache.gravitino.idp.model.IdpUser;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestBasicAuthentication {

  private static final String USER = "alice";
  private static final String PASSWORD = "Passw0rd-For-Alice";

  @Test
  public void testFilterSuccess() throws Exception {
    BasicAuthenticator authenticator = aliceAuthenticator();
    FilterChain chain = mock(FilterChain.class);
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    stubAuthHeader(request, USER, PASSWORD);

    new AuthenticationFilter(Lists.newArrayList(authenticator)).doFilter(request, response, chain);

    verify(chain).doFilter(request, response);
    verify(response, never()).sendError(anyInt(), anyString());
    ArgumentCaptor<Object> principalCaptor = ArgumentCaptor.forClass(Object.class);
    verify(request)
        .setAttribute(
            eq(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME), principalCaptor.capture());
    assertEquals(USER, ((UserPrincipal) principalCaptor.getValue()).getName());
  }

  @Test
  public void testFilterUnauthorized() throws Exception {
    IdpUserGroupManager userGroupManager = mock(IdpUserGroupManager.class);
    when(userGroupManager.authenticate(USER, "wrong")).thenReturn(null);
    BasicAuthenticator authenticator = createBasicAuthenticator(userGroupManager);
    FilterChain chain = mock(FilterChain.class);
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    stubAuthHeader(request, USER, "wrong");

    new AuthenticationFilter(Lists.newArrayList(authenticator)).doFilter(request, response, chain);

    verify(response).setHeader(AuthConstants.HTTP_CHALLENGE_HEADER, "Basic");
    verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid username or password");
    verify(chain, never()).doFilter(request, response);
  }

  private static BasicAuthenticator aliceAuthenticator() throws Exception {
    IdpUserGroupManager userGroupManager = mock(IdpUserGroupManager.class);
    when(userGroupManager.authenticate(USER, PASSWORD))
        .thenReturn(new IdpUser(USER, Collections.emptyList()));
    return createBasicAuthenticator(userGroupManager);
  }

  private static void stubAuthHeader(HttpServletRequest request, String username, String password) {
    when(request.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(
            new Vector<>(Collections.singletonList(basicAuthHeader(username, password)))
                .elements());
  }

  private static BasicAuthenticator createBasicAuthenticator(IdpUserGroupManager userGroupManager)
      throws Exception {
    BasicAuthenticator authenticator = new BasicAuthenticator();
    Field field = BasicAuthenticator.class.getDeclaredField("userGroupManager");
    field.setAccessible(true);
    field.set(authenticator, userGroupManager);
    return authenticator;
  }

  private static String basicAuthHeader(String username, String password) {
    return AuthConstants.AUTHORIZATION_BASIC_HEADER
        + Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
