/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.datastrato.graviton.server.web.VersioningFilter.MutableHttpServletRequest;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestVersioningFilter {

  @Test
  public void testDoFilterWithSupportedVersion() throws ServletException, IOException {
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);

    when(mockRequest.getHeaders("Accept"))
        .thenReturn(
            new Vector<>(Collections.singletonList("application/vnd.graviton.v1+json")).elements());

    filter.doFilter(mockRequest, mockResponse, mockChain);

    verify(mockChain).doFilter(any(), any());
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testDoFilterWithUnsupportedVersion() throws ServletException, IOException {
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);

    when(mockRequest.getHeaders("Accept"))
        .thenReturn(
            new Vector<>(Collections.singletonList("application/vnd.graviton.v2+json")).elements());

    filter.doFilter(mockRequest, mockResponse, mockChain);

    verify(mockChain, never()).doFilter(any(), any());
    verify(mockResponse).sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, "Unsupported version");
  }

  @Test
  public void testDoFilterWithNoVersionHeader() throws ServletException, IOException {
    // Arrange
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);

    when(mockRequest.getHeaders("Accept"))
        .thenReturn(new Vector<>(Collections.singletonList("some other header")).elements());

    filter.doFilter(mockRequest, mockResponse, mockChain);

    verify(mockChain).doFilter(any(), any());
    verify(mockResponse, never()).sendError(anyInt(), anyString());

    ArgumentCaptor<MutableHttpServletRequest> captor =
        ArgumentCaptor.forClass(MutableHttpServletRequest.class);
    verify(mockChain).doFilter(captor.capture(), any());
    assertEquals(
        String.format("application/vnd.graviton.v%d+json", ApiVersion.latestVersion().version()),
        captor.getValue().getHeader("Accept"));
  }

  @Test
  public void testDoFilterWithNoHeaders() throws ServletException, IOException {
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);

    when(mockRequest.getHeaders("Accept")).thenReturn(Collections.emptyEnumeration());

    filter.doFilter(mockRequest, mockResponse, mockChain);

    verify(mockChain).doFilter(any(), any());
    verify(mockResponse, never()).sendError(anyInt(), anyString());

    ArgumentCaptor<MutableHttpServletRequest> captor =
        ArgumentCaptor.forClass(MutableHttpServletRequest.class);
    verify(mockChain).doFilter(captor.capture(), any());
    assertEquals(
        String.format("application/vnd.graviton.v%d+json", ApiVersion.latestVersion().version()),
        captor.getValue().getHeader("Accept"));
  }

  @Test
  public void testDoFilterWithMultipleAcceptHeaders() throws ServletException, IOException {
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);

    when(mockRequest.getHeaders("Accept"))
        .thenReturn(
            new Vector<>(Collections.singletonList("application/vnd.graviton.v1+json")).elements(),
            new Vector<>(Collections.singletonList("application/vnd.graviton.v2+json")).elements());

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain).doFilter(any(), any());

    reset(mockChain, mockResponse);

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain, never()).doFilter(any(), any());
    verify(mockResponse).sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, "Unsupported version");
  }

  @Test
  public void testDoFilterWithInvalidAcceptHeaderFormat() throws ServletException, IOException {
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);

    when(mockRequest.getHeaders("Accept"))
        .thenReturn(new Vector<>(Collections.singletonList("invalid-format")).elements());

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain).doFilter(any(), any());

    ArgumentCaptor<MutableHttpServletRequest> captor =
        ArgumentCaptor.forClass(MutableHttpServletRequest.class);
    verify(mockChain).doFilter(captor.capture(), any());
    assertEquals(
        String.format("application/vnd.graviton.v%d+json", ApiVersion.latestVersion().version()),
        captor.getValue().getHeader("Accept"));
  }

  @Test
  public void testDoFilterWithNullRequest() throws ServletException, IOException {
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);

    assertThrows(NullPointerException.class, () -> filter.doFilter(null, mockResponse, mockChain));
  }

  @Test
  public void testDoFilterWithNullResponse() throws ServletException, IOException {
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);

    assertThrows(NullPointerException.class, () -> filter.doFilter(mockRequest, null, mockChain));
  }

  @Test
  public void testDoFilterWithValidAndInvalidVersionHeaders() throws ServletException, IOException {
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);

    when(mockRequest.getHeaders("Accept"))
        .thenReturn(
            new Vector<>(Collections.singletonList("application/vnd.graviton.v1+json")).elements(),
            new Vector<>(Collections.singletonList("invalid-version")).elements());

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain).doFilter(any(), any());

    reset(mockChain, mockResponse);

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain).doFilter(any(), any());
  }

  @Test
  public void testDoFilterWithMultipleVersions() throws ServletException, IOException {
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);

    when(mockRequest.getHeaders("Accept"))
        .thenReturn(
            new Vector<>(Collections.singletonList("application/vnd.graviton.v1+json")).elements(),
            new Vector<>(Collections.singletonList("application/vnd.graviton.v2+json")).elements(),
            new Vector<>(Collections.singletonList("application/vnd.graviton.v3+json")).elements());

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain).doFilter(any(), any());

    reset(mockChain, mockResponse);

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain, never()).doFilter(any(), any());
    verify(mockResponse).sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, "Unsupported version");

    reset(mockChain, mockResponse);

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain, never()).doFilter(any(), any());
    verify(mockResponse).sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, "Unsupported version");
  }

  @Test
  public void testGetHeaderNames() {
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    Enumeration<String> mockHeaderNames =
        new Vector<>(Arrays.asList("Header1", "Header2")).elements();
    when(mockRequest.getHeaderNames()).thenReturn(mockHeaderNames);

    VersioningFilter.MutableHttpServletRequest mutableRequest =
        new VersioningFilter.MutableHttpServletRequest(mockRequest);
    mutableRequest.putHeader("CustomHeader", "Value");
    Enumeration<String> headerNames = mutableRequest.getHeaderNames();
    List<String> actualHeaderNames = Collections.list(headerNames);

    assertEquals(3, actualHeaderNames.size());
    assertTrue(actualHeaderNames.contains("Header1"));
    assertTrue(actualHeaderNames.contains("Header2"));
    assertTrue(actualHeaderNames.contains("CustomHeader"));
  }
}
