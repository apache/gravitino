/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.authentication;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
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
