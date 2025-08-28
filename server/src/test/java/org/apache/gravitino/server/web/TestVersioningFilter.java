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
package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.apache.gravitino.server.web.VersioningFilter.MutableHttpServletRequest;
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
            new Vector<>(Collections.singletonList("application/vnd.gravitino.v1+json"))
                .elements());

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
            new Vector<>(Collections.singletonList("application/vnd.gravitino.v2+json"))
                .elements());

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
        String.format("application/vnd.gravitino.v%d+json", ApiVersion.latestVersion().version()),
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
        String.format("application/vnd.gravitino.v%d+json", ApiVersion.latestVersion().version()),
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
            new Vector<>(Collections.singletonList("application/vnd.gravitino.v1+json")).elements(),
            new Vector<>(Collections.singletonList("application/vnd.gravitino.v2+json"))
                .elements());

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
        String.format("application/vnd.gravitino.v%d+json", ApiVersion.latestVersion().version()),
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
            new Vector<>(Collections.singletonList("application/vnd.gravitino.v1+json")).elements(),
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
            new Vector<>(Collections.singletonList("application/vnd.gravitino.v1+json")).elements(),
            new Vector<>(Collections.singletonList("application/vnd.gravitino.v2+json")).elements(),
            new Vector<>(Collections.singletonList("application/vnd.gravitino.v3+json"))
                .elements());

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

  @Test
  public void testDoFilterWithHeaderContainingValidVersionAsSubstring() throws Exception {
    VersioningFilter filter = new VersioningFilter();
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders("Accept"))
        .thenReturn(
            new Vector<>(
                    Collections.singletonList(
                        "application/vnd.gravitino.v1+json, application/json"))
                .elements());

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain).doFilter(any(), any());

    reset(mockChain, mockResponse);

    when(mockRequest.getHeaders("Accept"))
        .thenReturn(
            new Vector<>(
                    Collections.singletonList(
                        "application/vnd.gravitino.v2+json, application/json"))
                .elements());

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain, never()).doFilter(any(), any());
    verify(mockResponse).sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, "Unsupported version");

    reset(mockChain, mockResponse);

    when(mockRequest.getHeaders("Accept"))
        .thenReturn(
            new Vector<>(
                    Collections.singletonList(
                        "application/vnd.gravitino.v3+json; q=0.9, application/json"))
                .elements());
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockChain, never()).doFilter(any(), any());
    verify(mockResponse).sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, "Unsupported version");
  }
}
