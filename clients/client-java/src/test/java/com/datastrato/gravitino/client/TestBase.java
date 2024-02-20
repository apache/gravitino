/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;

public abstract class TestBase {

  private static final ObjectMapper MAPPER = JsonUtils.objectMapper();

  private static ClientAndServer mockServer;

  protected static GravitinoClient client;

  @BeforeAll
  public static void setUp() throws Exception {
    mockServer = ClientAndServer.startClientAndServer(0);
    int port = mockServer.getLocalPort();
    client = GravitinoClient.builder("http://127.0.0.1:" + port).build();
  }

  @AfterAll
  public static void tearDown() {
    mockServer.stop();
    client.close();
  }

  protected static <T, R> void buildMockResource(
      Method method,
      String path,
      Map<String, String> queryParams,
      T reqBody,
      R respBody,
      int statusCode)
      throws JsonProcessingException {
    List<Parameter> parameters =
        queryParams.entrySet().stream()
            .map(kv -> new Parameter(kv.getKey(), kv.getValue()))
            .collect(Collectors.toList());

    HttpRequest mockRequest =
        HttpRequest.request(path).withMethod(method.name()).withQueryStringParameters(parameters);
    if (reqBody != null) {
      String reqJson = MAPPER.writeValueAsString(reqBody);
      mockRequest = mockRequest.withBody(reqJson);
    }

    HttpResponse mockResponse = HttpResponse.response().withStatusCode(statusCode);
    if (respBody != null) {
      String respJson = MAPPER.writeValueAsString(respBody);
      mockResponse = mockResponse.withBody(respJson);
    }

    // Using Times.exactly(1) will only match once for the request, so we could set difference
    // responses for the same request and path.
    mockServer.when(mockRequest, Times.exactly(1)).respond(mockResponse);
  }

  protected static <T, R> void buildMockResource(
      Method method, String path, T reqBody, R respBody, int statusCode)
      throws JsonProcessingException {
    buildMockResource(method, path, Collections.emptyMap(), reqBody, respBody, statusCode);
  }

  protected static String withSlash(String path) {
    return "/" + path;
  }
}
