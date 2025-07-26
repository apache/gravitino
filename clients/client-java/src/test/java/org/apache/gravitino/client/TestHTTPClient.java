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
package org.apache.gravitino.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.rest.RESTResponse;
import org.apache.hc.client5.http.config.ConnectionConfig;
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

  private static final ObjectMapper MAPPER = ObjectMapperProvider.objectMapper();

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

  @Test
  public void testNullHeadersDoNotCauseNPE() throws Exception {
    Item body = new Item(0L, "hank");
    int statusCode = 200;

    ErrorHandler onError = mock(ErrorHandler.class);
    doThrow(new RuntimeException("Failure response")).when(onError).accept(any());

    String path = addRequestTestCaseAndGetPath(Method.GET, body, statusCode, false, true);

    Item response = restClient.get(path, Item.class, (Map<String, String>) null, onError);

    Assertions.assertEquals(body, response);
    verify(onError, never()).accept(any());
  }

  @Test
  public void testSocketAndConnectionTimeoutSet() {
    // test default value
    ConnectionConfig connectionConfigWithDefault =
        HTTPClient.configureConnectionConfig(
            GravitinoClientConfiguration.buildFromProperties(ImmutableMap.of()));
    Assertions.assertNotNull(connectionConfigWithDefault);
    Assertions.assertEquals(
        connectionConfigWithDefault.getConnectTimeout().getDuration(),
        GravitinoClientConfiguration.CLIENT_CONNECTION_TIMEOUT_MS_DEFAULT);
    Assertions.assertEquals(
        connectionConfigWithDefault.getSocketTimeout().getDuration(),
        GravitinoClientConfiguration.CLIENT_SOCKET_TIMEOUT_MS_DEFAULT);

    // test custom value
    long connectionTimeoutMs = 10L;
    int socketTimeoutMs = 10;
    Map<String, String> properties =
        ImmutableMap.of(
            "gravitino.client.connectionTimeoutMs", String.valueOf(connectionTimeoutMs),
            "gravitino.client.socketTimeoutMs", String.valueOf(socketTimeoutMs));

    ConnectionConfig connectionConfig =
        HTTPClient.configureConnectionConfig(
            GravitinoClientConfiguration.buildFromProperties(properties));
    Assertions.assertNotNull(connectionConfig);
    Assertions.assertEquals(
        connectionConfig.getConnectTimeout().getDuration(), connectionTimeoutMs);
    Assertions.assertEquals(connectionConfig.getSocketTimeout().getDuration(), socketTimeoutMs);

    // test invalid value
    Map<String, String> propertiesWithInvalidConnectionTimeoutMs =
        ImmutableMap.of("gravitino.client.connectionTimeoutMs", "-1");
    try {
      HTTPClient.configureConnectionConfig(
          GravitinoClientConfiguration.buildFromProperties(
              propertiesWithInvalidConnectionTimeoutMs));
    } catch (IllegalArgumentException e) {
      Assertions.assertEquals(
          "-1 in gravitino.client.connectionTimeoutMs is invalid. The value must be a positive number",
          e.getMessage());
    }

    Map<String, String> propertiesWithInvalidSocketTimeoutMs =
        ImmutableMap.of("gravitino.client.socketTimeoutMs", "aaaa");
    try {
      HTTPClient.configureConnectionConfig(
          GravitinoClientConfiguration.buildFromProperties(propertiesWithInvalidSocketTimeoutMs));
    } catch (IllegalArgumentException e) {
      Assertions.assertEquals(
          "aaaa in gravitino.client.socketTimeoutMs is invalid. The value must be an integer number",
          e.getMessage());
    }
  }

  @Test
  public void testConnectionTimeout() throws IOException {
    String path = "test_connection_timeout";
    long connectionTimeoutMs = 2000;
    Map<String, String> properties =
        ImmutableMap.of(
            "gravitino.client.connectionTimeoutMs", String.valueOf(connectionTimeoutMs));
    // use error server port to simulate hitting the configured connection timeout of 2 seconds
    try (HTTPClient client =
        HTTPClient.builder(properties)
            .uri(String.format("http://127.0.0.1:%d", mockServer.getPort() + 1))
            .build()) {
      HttpRequest mockRequest =
          request().withPath("/" + path).withMethod(Method.HEAD.name().toUpperCase(Locale.ROOT));
      HttpResponse mockResponse = response().withStatusCode(200);
      mockServer.when(mockRequest).respond(mockResponse);

      long start = System.currentTimeMillis();
      Assertions.assertThrows(
          RESTException.class, () -> client.head(path, ImmutableMap.of(), response -> {}));
      long end = System.currentTimeMillis();
      Assertions.assertTrue((end - start) < 5000);
    }
  }

  @Test
  public void testSocketTimeout() throws IOException {
    String path = "test_socket_timeout";
    long socketTimeoutMs = 2000;
    Map<String, String> properties =
        ImmutableMap.of("gravitino.client.socketTimeoutMs", String.valueOf(socketTimeoutMs));
    try (HTTPClient client =
        HTTPClient.builder(properties)
            .uri(String.format("http://127.0.0.1:%d", mockServer.getPort()))
            .build()) {
      HttpRequest mockRequest =
          request().withPath("/" + path).withMethod(Method.HEAD.name().toUpperCase(Locale.ROOT));
      // Setting a response delay of 5 seconds to simulate hitting the configured socket timeout of
      // 2 seconds
      HttpResponse mockResponse =
          response()
              .withStatusCode(200)
              .withBody("Delayed response")
              .withDelay(TimeUnit.MILLISECONDS, 5000);
      mockServer.when(mockRequest).respond(mockResponse);

      Throwable throwable =
          Assertions.assertThrows(
              RESTException.class, () -> client.head(path, ImmutableMap.of(), response -> {}));
      Assertions.assertInstanceOf(SocketTimeoutException.class, throwable.getCause());
      Assertions.assertEquals("Read timed out", throwable.getCause().getMessage());
    }
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
