package com.datastrato.gravitino.server.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

import com.datastrato.gravitino.auth.Authenticator;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.datastrato.gravitino.utils.Constants;
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
    when(mockRequest.getHeaders(Constants.HTTP_HEADER_NAME))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.isDataFromHTTP()).thenReturn(true);
    when(authenticator.authenticateHTTPHeader(any())).thenReturn("user");
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
    when(mockRequest.getHeaders(Constants.HTTP_HEADER_NAME))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.isDataFromHTTP()).thenReturn(true);
    when(authenticator.authenticateHTTPHeader(any()))
        .thenThrow(new UnauthorizedException("UNAUTHORIZED"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED, "UNAUTHORIZED");
  }
}
