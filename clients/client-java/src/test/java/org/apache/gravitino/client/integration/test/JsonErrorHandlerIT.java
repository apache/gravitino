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
package org.apache.gravitino.client.integration.test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test verifying that requests rejected before reaching a resource method (malformed
 * typed parameters, unmatched routes, wrong HTTP methods, an unsupported API version) return the
 * same structured JSON {@link ErrorResponse} used by every other error on the API, instead of
 * Jetty's default HTML error page.
 *
 * <p>Most of these cases are handled by Jersey itself — converting a typed
 * {@code @PathParam}/{@code @QueryParam}, matching a route, matching an HTTP method to a resource —
 * before any resource method runs. The unsupported-API-version case is rejected earlier still, by
 * {@code VersioningFilter} before the request ever reaches Jersey. Either way, the
 * metalake/catalog/schema/model in the URL need not actually exist for these failures to occur.
 */
public class JsonErrorHandlerIT extends BaseIT {

  private final HttpClient httpClient = HttpClient.newHttpClient();

  @Test
  public void testMalformedModelVersionReturnsJsonErrorBody() throws Exception {
    HttpResponse<String> response =
        sendRequest("GET", "/api/metalakes/m/catalogs/c/schemas/s/models/mo/versions/abc");

    Assertions.assertEquals(404, response.statusCode());
    assertJsonErrorBody(response, ErrorConstants.NOT_FOUND_CODE, "PathParamException");
  }

  @Test
  public void testMalformedModelVersionUriReturnsJsonErrorBody() throws Exception {
    HttpResponse<String> response =
        sendRequest("GET", "/api/metalakes/m/catalogs/c/schemas/s/models/mo/versions/abc/uri");

    Assertions.assertEquals(404, response.statusCode());
    assertJsonErrorBody(response, ErrorConstants.NOT_FOUND_CODE, "PathParamException");
  }

  @Test
  public void testUnknownApiRouteStillReturnsJsonErrorBody() throws Exception {
    // A route that Jersey cannot match at all is a different failure (no @PathParam conversion
    // is even attempted), but it must be covered by the same fix.
    HttpResponse<String> response = sendRequest("GET", "/api/v99/nonexistent/route");

    Assertions.assertEquals(404, response.statusCode());
    assertJsonErrorBody(response, ErrorConstants.NOT_FOUND_CODE, "NotFoundException");
  }

  @Test
  public void testWrongHttpMethodReturnsJsonErrorBody() throws Exception {
    // /api/version only supports GET; POSTing to it never reaches a resource method either.
    HttpResponse<String> response = sendRequest("POST", "/api/version");

    Assertions.assertEquals(405, response.statusCode());
    assertJsonErrorBody(
        response, ErrorConstants.UNSUPPORTED_OPERATION_CODE, "UnsupportedOperationException");
  }

  @Test
  public void testUnsupportedApiVersionReturnsJsonErrorBody() throws Exception {
    // VersioningFilter rejects an unrecognized API version before the request ever reaches
    // Jersey, so this is not covered by any JAX-RS ExceptionMapper.
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(new URI("http://localhost:" + getGravitinoServerPort() + "/api/version"))
            .header("Accept", "application/vnd.gravitino.v99+json")
            .GET()
            .build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    Assertions.assertEquals(406, response.statusCode());
    assertJsonErrorBody(
        response, ErrorConstants.ILLEGAL_ARGUMENTS_CODE, "IllegalArgumentException");
  }

  private HttpResponse<String> sendRequest(String method, String path) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(new URI("http://localhost:" + getGravitinoServerPort() + path))
            .method(method, HttpRequest.BodyPublishers.noBody())
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private void assertJsonErrorBody(
      HttpResponse<String> response, int expectedCode, String expectedType) throws Exception {
    String contentType = response.headers().firstValue("Content-Type").orElse("");
    Assertions.assertTrue(
        contentType.contains("application/json"),
        "Expected a JSON error body, got Content-Type: " + contentType);
    Assertions.assertFalse(
        response.body().contains("<html"), "Response body must not be Jetty's HTML error page");

    ErrorResponse errorResponse =
        ObjectMapperProvider.objectMapper().readValue(response.body(), ErrorResponse.class);
    Assertions.assertEquals(expectedCode, errorResponse.getCode());
    Assertions.assertEquals(expectedType, errorResponse.getType());
    Assertions.assertFalse(errorResponse.getMessage().isEmpty());
  }
}
