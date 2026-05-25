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
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Vector;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.idp.auth.BasicAuthenticator;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestBasicAuthentication {

  private static final String USER = "alice";
  private static final String PASSWORD = "Passw0rd-For-Alice";
  private static final String PASSWORD_HASH = "hash-1";

  @Test
  public void testFilterSuccess() throws Exception {
    BasicAuthenticator authenticator = aliceAuthenticator(true);
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
    IdpUserMetaService userMetaService = mock(IdpUserMetaService.class);
    when(userMetaService.getIdpUserByUsername(USER))
        .thenThrow(new NotFoundException("IdP user not found: %s", USER));
    BasicAuthenticator authenticator =
        createBasicAuthenticator(userMetaService, mock(PasswordHasher.class));
    FilterChain chain = mock(FilterChain.class);
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    stubAuthHeader(request, USER, "wrong");

    new AuthenticationFilter(Lists.newArrayList(authenticator)).doFilter(request, response, chain);

    verify(response).setHeader(AuthConstants.HTTP_CHALLENGE_HEADER, "Basic");
    verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid username or password");
    verify(chain, never()).doFilter(request, response);
  }

  private static BasicAuthenticator aliceAuthenticator(boolean passwordValid) throws Exception {
    IdpUserMetaService userMetaService = mock(IdpUserMetaService.class);
    PasswordHasher passwordHasher = mock(PasswordHasher.class);
    IdpUserPO userPO = mock(IdpUserPO.class);
    when(userMetaService.getIdpUserByUsername(USER)).thenReturn(userPO);
    when(userPO.getPasswordHash()).thenReturn(PASSWORD_HASH);
    when(passwordHasher.verify(PASSWORD, PASSWORD_HASH)).thenReturn(passwordValid);
    when(userMetaService.listGroupNamesByUsername(USER)).thenReturn(Collections.emptyList());
    return createBasicAuthenticator(userMetaService, passwordHasher);
  }

  private static void stubAuthHeader(HttpServletRequest request, String username, String password) {
    when(request.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(
            new Vector<>(Collections.singletonList(basicAuthHeader(username, password)))
                .elements());
  }

  private static BasicAuthenticator createBasicAuthenticator(
      IdpUserMetaService userMetaService, PasswordHasher passwordHasher) throws Exception {
    Constructor<BasicAuthenticator> constructor =
        BasicAuthenticator.class.getDeclaredConstructor(
            IdpUserMetaService.class, PasswordHasher.class);
    constructor.setAccessible(true);
    return constructor.newInstance(userMetaService, passwordHasher);
  }

  private static String basicAuthHeader(String username, String password) {
    return AuthConstants.AUTHORIZATION_BASIC_HEADER
        + Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
