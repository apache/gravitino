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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.Vector;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.junit.jupiter.api.Test;

public class TestAuthenticationFilter {

  @Test
  public void testDoFilterNormal() throws ServletException, IOException {

    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testDoFilterWithException() throws ServletException, IOException {
    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any()))
        .thenThrow(new UnauthorizedException("UNAUTHORIZED"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED, "UNAUTHORIZED");
  }

  @Test
  public void testMultiFilterNormal() throws ServletException, IOException {

    Authenticator authenticator1 = mock(Authenticator.class);
    Authenticator authenticator2 = mock(Authenticator.class);
    AuthenticationFilter filter =
        new AuthenticationFilter(Lists.newArrayList(authenticator1, authenticator2));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator1.supportsToken(any())).thenReturn(false);
    when(authenticator1.isDataFromToken()).thenReturn(true);
    when(authenticator1.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    when(authenticator2.supportsToken(any())).thenReturn(true);
    when(authenticator2.isDataFromToken()).thenReturn(true);
    when(authenticator2.authenticateToken(any())).thenReturn(new UserPrincipal("user"));

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testMultiFilterWithException() throws ServletException, IOException {

    Authenticator authenticator1 = mock(Authenticator.class);
    Authenticator authenticator2 = mock(Authenticator.class);
    AuthenticationFilter filter =
        new AuthenticationFilter(Lists.newArrayList(authenticator1, authenticator2));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator1.supportsToken(any())).thenReturn(false);
    when(authenticator1.isDataFromToken()).thenReturn(true);
    when(authenticator1.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    when(authenticator2.supportsToken(any())).thenReturn(true);
    when(authenticator2.isDataFromToken()).thenReturn(true);
    when(authenticator2.authenticateToken(any()))
        .thenThrow(new UnauthorizedException("UNAUTHORIZED"));

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED, "UNAUTHORIZED");
  }
}
