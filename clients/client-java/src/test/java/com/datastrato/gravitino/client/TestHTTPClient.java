/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastrato.gravitino.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.exceptions.NotFoundException;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rest.RESTRequest;
import com.datastrato.gravitino.rest.RESTResponse;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

/**
 * * Exercises the RESTClient interface, specifically over a mocked-server using the actual
 * HttpRESTClient code.
 *
 * <p>Referred from core/src/test/java/org/apache/iceberg/rest/TestHTTPClient.java
 */
public class TestHTTPClient {

  private static final ObjectMapper MAPPER = JsonUtils.objectMapper();

  private static ClientAndServer mockServer;
  private static RESTClient restClient;

  @BeforeAll
  public static void beforeClass() {
    mockServer = startClientAndServer();
    restClient =
        HTTPClient.builder(ImmutableMap.of())
            .uri(String.format("http://127.0.0.1:%d", mockServer.getPort()))
            .build();
  }

  @AfterAll
  public static void stopServer() throws IOException {
    mockServer.stop();
    restClient.close();
  }

  @Test
  public void testPostSuccess() throws Exception {
    testHttpMethodOnSuccess(Method.POST, true, true);
  }

  @Test
  public void testPostFailure() throws Exception {
    testHttpMethodOnFailure(Method.POST, true, true);
  }

  @Test
  public void testPutSuccess() throws Exception {
    testHttpMethodOnSuccess(Method.PUT, true, true);
  }

  @Test
  public void testPutFailure() throws Exception {
    testHttpMethodOnFailure(Method.PUT, true, true);
  }

  @Test
  public void testGetSuccess() throws Exception {
    testHttpMethodOnSuccess(Method.GET, false, true);
  }

  @Test
  public void testGetFailure() throws Exception {
    testHttpMethodOnFailure(Method.GET, false, true);
  }

  @Test
  public void testDeleteSuccess() throws Exception {
    testHttpMethodOnSuccess(Method.DELETE, false, true);
  }

  @Test
  public void testDeleteFailure() throws Exception {
    testHttpMethodOnFailure(Method.DELETE, false, true);
  }

  @Test
  public void testHeadSuccess() throws JsonProcessingException {
    testHttpMethodOnSuccess(Method.HEAD, false, false);
  }

  @Test
  public void testHeadFailure() throws JsonProcessingException {
    testHttpMethodOnFailure(Method.HEAD, false, false);
  }

  public static void testHttpMethodOnSuccess(
      Method method, boolean hasRequestBody, boolean hasResponseBody)
      throws JsonProcessingException {
    Item body = new Item(0L, "hank");
    int statusCode = 200;

    ErrorHandler onError = mock(ErrorHandler.class);
    doThrow(new RuntimeException("Failure response")).when(onError).accept(any());

    String path =
        addRequestTestCaseAndGetPath(method, body, statusCode, hasRequestBody, hasResponseBody);

    Item successResponse = doExecuteRequest(method, path, body, onError, h -> {});

    if (hasResponseBody) {
      Assertions.assertEquals(successResponse, body);
    }

    verify(onError, never()).accept(any());
  }

  public static void testHttpMethodOnFailure(
      Method method, boolean hasRequestBody, boolean hasResponseBody)
      throws JsonProcessingException {
    Item body = new Item(0L, "hank");
    int statusCode = 404;

    ErrorHandler onError = mock(ErrorHandler.class);
    doThrow(
            new RuntimeException(
                String.format(
                    "Called error handler for method %s due to status code: %d",
                    method, statusCode)))
        .when(onError)
        .accept(any());

    String path =
        addRequestTestCaseAndGetPath(method, body, statusCode, hasRequestBody, hasResponseBody);

    Throwable exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> doExecuteRequest(method, path, body, onError, h -> {}));
    Assertions.assertEquals(
        String.format(
            "Called error handler for method %s due to status code: %d", method.name(), statusCode),
        exception.getMessage());

    verify(onError).accept(any());
  }

  // Adds a request that the mock-server can match against, based on the method, path, body, and
  // headers.
  // Return the path generated for the test case, so that the client can call that path to exercise
  // it.
  private static String addRequestTestCaseAndGetPath(
      Method method, Item body, int statusCode, boolean hasRequestBody, boolean hasResponseBody)
      throws JsonProcessingException {

    // Build the path route, which must be unique per test case.
    boolean isSuccess = statusCode == 200;
    // Using different paths keeps the expectations unique for the test's mock server
    String pathName = isSuccess ? "success" : "failure";
    String path = String.format("%s_%s", method, pathName);

    // Build the expected request
    String asJson = body != null ? MAPPER.writeValueAsString(body) : null;
    HttpRequest mockRequest =
        request("/" + path).withMethod(method.name().toUpperCase(Locale.ROOT));

    if (hasRequestBody) {
      mockRequest = mockRequest.withBody(asJson);
    }

    // Build the expected response
    HttpResponse mockResponse = response().withStatusCode(statusCode);

    if (hasResponseBody) {
      if (isSuccess) {
        // Simply return the passed in item in the success case.
        mockResponse = mockResponse.withBody(asJson);
      } else {
        ErrorResponse response =
            ErrorResponse.notFound(NotFoundException.class.getSimpleName(), "Not found");
        mockResponse = mockResponse.withBody(MAPPER.writeValueAsString(response));
      }
    }

    mockServer.when(mockRequest).respond(mockResponse);

    return path;
  }

  private static Item doExecuteRequest(
      Method method,
      String path,
      Item body,
      ErrorHandler onError,
      Consumer<Map<String, String>> responseHeaders) {
    Map<String, String> headers = ImmutableMap.of();
    switch (method) {
      case POST:
        return restClient.post(path, body, Item.class, headers, onError, responseHeaders);
      case PUT:
        return restClient.put(path, body, Item.class, headers, onError, responseHeaders);
      case GET:
        return restClient.get(path, Item.class, headers, onError);
      case HEAD:
        restClient.head(path, headers, onError);
        return null;
      case DELETE:
        return restClient.delete(path, Item.class, () -> headers, onError);
      default:
        throw new IllegalArgumentException(String.format("Invalid method: %s", method));
    }
  }

  public static class Item implements RESTRequest, RESTResponse {
    @JsonProperty private Long id;
    @JsonProperty private String data;

    // Required for Jackson deserialization
    @SuppressWarnings("unused")
    public Item() {}

    public Item(Long id, String data) {
      this.id = id;
      this.data = data;
    }

    @Override
    public void validate() {}

    @Override
    public int hashCode() {
      return Objects.hash(id, data);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Item)) {
        return false;
      }
      Item item = (Item) o;
      return Objects.equals(id, item.id) && Objects.equals(data, item.data);
    }
  }
}
