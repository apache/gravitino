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

package com.apache.gravitino.server.authentication;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.apache.gravitino.auth.AuthConstants;
import com.apache.gravitino.exceptions.UnauthorizedException;
import com.datastrato.gravitino.UserPrincipal;
import java.io.IOException;
import java.util.Collections;
import java.util.Vector;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

public class TestAuthenticationFilter {

  @Test
  public void testDoFilterNormal() throws ServletException, IOException {

    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(authenticator);
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testDoFilterWithException() throws ServletException, IOException {
    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(authenticator);
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any()))
        .thenThrow(new UnauthorizedException("UNAUTHORIZED"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED, "UNAUTHORIZED");
  }
}
