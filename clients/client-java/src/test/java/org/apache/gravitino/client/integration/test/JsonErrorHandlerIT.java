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
 * Integration test verifying that a malformed typed path parameter on the metadata API (e.g. a
 * non-numeric model version) returns the same structured JSON {@link ErrorResponse} used by every
 * other error on the API, instead of Jetty's default HTML error page.
 *
 * <p>Jersey fails to convert the path segment into the resource method's typed {@code @PathParam}
 * before any resource method runs, so the metalake/catalog/schema/model in the URL need not
 * actually exist for this failure to occur.
 */
public class JsonErrorHandlerIT extends BaseIT {

  private final HttpClient httpClient = HttpClient.newHttpClient();

  @Test
  public void testMalformedModelVersionReturnsJsonErrorBody() throws Exception {
    HttpResponse<String> response =
        sendGet("/api/metalakes/m/catalogs/c/schemas/s/models/mo/versions/abc");

    Assertions.assertEquals(404, response.statusCode());
    assertJsonNotFoundBody(response, "PathParamException");
  }

  @Test
  public void testMalformedModelVersionUriReturnsJsonErrorBody() throws Exception {
    HttpResponse<String> response =
        sendGet("/api/metalakes/m/catalogs/c/schemas/s/models/mo/versions/abc/uri");

    Assertions.assertEquals(404, response.statusCode());
    assertJsonNotFoundBody(response, "PathParamException");
  }

  @Test
  public void testUnknownApiRouteStillReturnsJsonErrorBody() throws Exception {
    // A route that Jersey cannot match at all is a different failure (no @PathParam conversion
    // is even attempted), but it must be covered by the same fix.
    HttpResponse<String> response = sendGet("/api/v99/nonexistent/route");

    Assertions.assertEquals(404, response.statusCode());
    assertJsonNotFoundBody(response, "NotFoundException");
  }

  private HttpResponse<String> sendGet(String path) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(new URI("http://localhost:" + getGravitinoServerPort() + path))
            .GET()
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private void assertJsonNotFoundBody(HttpResponse<String> response, String expectedType)
      throws Exception {
    String contentType = response.headers().firstValue("Content-Type").orElse("");
    Assertions.assertTrue(
        contentType.contains("application/json"),
        "Expected a JSON error body, got Content-Type: " + contentType);
    Assertions.assertFalse(
        response.body().contains("<html"), "Response body must not be Jetty's HTML error page");

    ErrorResponse errorResponse =
        ObjectMapperProvider.objectMapper().readValue(response.body(), ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(expectedType, errorResponse.getType());
    Assertions.assertFalse(errorResponse.getMessage().isEmpty());
  }
}
