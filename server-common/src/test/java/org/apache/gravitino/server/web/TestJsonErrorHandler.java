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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.eclipse.jetty.server.Request;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestJsonErrorHandler {

  private final JsonErrorHandler handler = new JsonErrorHandler("/api/");

  @ParameterizedTest
  @CsvSource({
    "/api/metalakes/m/catalogs/c/schemas/s/models/mo/versions/abc, true",
    "/api/version, true",
    "/ui/index.html, false",
    "/configs, false"
  })
  public void testIsApiPath(String requestUri, boolean expected) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRequestURI()).thenReturn(requestUri);

    Assertions.assertEquals(expected, handler.isApiPath(request));
  }

  @ParameterizedTest
  @CsvSource({
    "400, IllegalArgumentException, 1001",
    "401, NotAuthorizedException,   1011",
    "403, ForbiddenException,       1008",
    "404, NotFoundException,        1003",
    "405, UnsupportedOperationException, 1006",
    "500, RuntimeException,         1002",
  })
  public void testHandleWritesStructuredJsonForApiPath(
      int status, String expectedType, int expectedCode) throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    Request baseRequest = mock(Request.class);

    when(request.getRequestURI())
        .thenReturn("/api/metalakes/m/catalogs/c/schemas/s/models/mo/versions/abc");
    when(response.getStatus()).thenReturn(status);

    StringWriter stringWriter = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(stringWriter));

    handler.handle("/error", baseRequest, request, response);

    verify(response).setContentType("application/json");
    verify(baseRequest).setHandled(true);

    ErrorResponse errorResponse =
        ObjectMapperProvider.objectMapper().readValue(stringWriter.toString(), ErrorResponse.class);
    Assertions.assertEquals(expectedType, errorResponse.getType());
    Assertions.assertEquals(expectedCode, errorResponse.getCode());
    Assertions.assertFalse(errorResponse.getMessage().isEmpty());
  }
}
