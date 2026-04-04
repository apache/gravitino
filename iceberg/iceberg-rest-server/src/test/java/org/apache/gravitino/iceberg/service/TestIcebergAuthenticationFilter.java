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
package org.apache.gravitino.iceberg.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergAuthenticationFilter {

  private static final ObjectMapper MAPPER = IcebergObjectMapper.getInstance();

  @Test
  public void testUnauthorizedErrorReturnsJson() throws Exception {
    IcebergAuthenticationFilter filter = new IcebergAuthenticationFilter();

    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(response.getWriter()).thenReturn(printWriter);

    filter.sendAuthErrorResponse(
        response, new UnauthorizedException("The provided credentials did not support"));

    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(response).setContentType("application/json");
    verify(response).setCharacterEncoding("UTF-8");

    printWriter.flush();
    String json = stringWriter.toString();
    ErrorResponse errorResponse = MAPPER.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(401, errorResponse.code());
    Assertions.assertEquals("NotAuthorizedException", errorResponse.type());
    Assertions.assertEquals("The provided credentials did not support", errorResponse.message());
  }

  @Test
  public void testInternalServerErrorReturnsJson() throws Exception {
    IcebergAuthenticationFilter filter = new IcebergAuthenticationFilter();

    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(response.getWriter()).thenReturn(printWriter);

    filter.sendAuthErrorResponse(response, new RuntimeException("Something went wrong"));

    verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);

    printWriter.flush();
    String json = stringWriter.toString();
    ErrorResponse errorResponse = MAPPER.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(500, errorResponse.code());
    Assertions.assertEquals("InternalServerError", errorResponse.type());
    Assertions.assertEquals("Something went wrong", errorResponse.message());
  }

  @Test
  public void testForbiddenExceptionReturnsJson() throws Exception {
    IcebergAuthenticationFilter filter = new IcebergAuthenticationFilter();

    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(response.getWriter()).thenReturn(printWriter);

    filter.sendAuthErrorResponse(
        response, new org.apache.gravitino.exceptions.ForbiddenException("Access denied"));

    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);

    printWriter.flush();
    String json = stringWriter.toString();
    ErrorResponse errorResponse = MAPPER.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(403, errorResponse.code());
    Assertions.assertEquals("NotAuthorizedException", errorResponse.type());
    Assertions.assertEquals("Access denied", errorResponse.message());
  }

  @Test
  public void testNullMessageUsesDefaultStatusMessage() throws Exception {
    IcebergAuthenticationFilter filter = new IcebergAuthenticationFilter();

    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(response.getWriter()).thenReturn(printWriter);

    filter.sendAuthErrorResponse(response, new RuntimeException((String) null));

    printWriter.flush();
    String json = stringWriter.toString();
    ErrorResponse errorResponse = MAPPER.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(500, errorResponse.code());
    Assertions.assertEquals("InternalServerError", errorResponse.type());
    Assertions.assertEquals("Server Error", errorResponse.message());
  }
}
