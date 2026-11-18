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
package org.apache.gravitino.client.integration.test.authorization;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Integration tests for invalid metadata object types in authorization-protected REST paths. */
public class InvalidMetadataObjectTypeAuthorizationIT extends BaseRestApiAuthorizationIT {

  /** Verifies that an invalid metadata object type is reported as malformed client input. */
  @Test
  public void testInvalidMetadataObjectTypeReturnsBadRequest() throws Exception {
    String authorization =
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder()
                .encodeToString((USER + ":dummy").getBytes(StandardCharsets.UTF_8));
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(new URI(serverUri + "/api/metalakes/zz/objects/bogusType/a.b.c/tags"))
            .header(AuthConstants.HTTP_HEADER_AUTHORIZATION, authorization)
            .GET()
            .build();

    HttpResponse<String> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

    Assertions.assertEquals(400, response.statusCode(), "Unexpected body: " + response.body());
    ErrorResponse errorResponse =
        ObjectMapperProvider.objectMapper().readValue(response.body(), ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), errorResponse.getType());
    Assertions.assertTrue(
        errorResponse.getMessage().contains("bogusType"),
        "Unexpected message: " + errorResponse.getMessage());
    Assertions.assertFalse(
        errorResponse.getMessage().contains("Authorization failed due to system internal error"),
        "Unexpected message: " + errorResponse.getMessage());
  }
}
