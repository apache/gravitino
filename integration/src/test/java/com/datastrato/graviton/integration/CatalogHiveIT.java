/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration;

import com.datastrato.graviton.client.ErrorHandlers;
import com.datastrato.graviton.client.HTTPClient;
import com.datastrato.graviton.client.RESTClient;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.requests.MetalakeCreateRequest;
import com.datastrato.graviton.dto.responses.ErrorResponse;
import com.datastrato.graviton.dto.responses.MetalakeListResponse;
import com.datastrato.graviton.dto.responses.MetalakeResponse;
import com.datastrato.graviton.integration.utils.GravitonITUtils;
import com.datastrato.graviton.rest.RESTResponse;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogHiveIT {
  public static final Logger LOG = LoggerFactory.getLogger(CatalogHiveIT.class);
  private static RESTClient restClient;
  private static final String URI = String.format("http://127.0.0.1:%d", 8090);

  @BeforeAll
  public static void startUp() {
    LOG.info("Starting up");
    GravitonITUtils.startGravitonServer();
    restClient = HTTPClient.builder(ImmutableMap.of()).uri(URI).build();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    LOG.info("Tearing down");
    restClient.close();
    GravitonITUtils.stopGravitonServer();
  }

  @Test
  public void createMetalakeTest() {
    MetalakeCreateRequest body =
        new MetalakeCreateRequest(
            GravitonITUtils.genRandomName(), "comment", ImmutableMap.of("key", "value"));
    String path = "api/metalakes";

    Consumer<ErrorResponse> onError = ErrorHandlers.restErrorHandler();
    MetalakeResponse successResponse =
        doExecuteRequest(Method.POST, path, body, MetalakeResponse.class, onError, h -> {});

    MetalakeListResponse listResponse =
        doExecuteRequest(Method.GET, path, body, MetalakeListResponse.class, onError, h -> {});

    List<MetalakeDTO> result =
        Arrays.stream(listResponse.getMetalakes())
            .filter(metalakeDTO -> metalakeDTO.name().equals(body.getName()))
            .collect(Collectors.toList());

    Assertions.assertEquals(result, 1);
  }

  private static <T extends RESTResponse> T doExecuteRequest(
      Method method,
      String path,
      MetalakeCreateRequest body,
      Class<T> responseType,
      Consumer<ErrorResponse> onError,
      Consumer<Map<String, String>> responseHeaders) {
    Map<String, String> headers = ImmutableMap.of();
    switch (method) {
      case POST:
        return restClient.post(path, body, responseType, headers, onError, responseHeaders);
      case PUT:
        return restClient.put(path, body, responseType, headers, onError, responseHeaders);
      case GET:
        return restClient.get(path, responseType, headers, onError);
      case HEAD:
        restClient.head(path, headers, onError);
        return null;
      case DELETE:
        return restClient.delete(path, responseType, () -> headers, onError);
      default:
        throw new IllegalArgumentException(String.format("Invalid method: %s", method));
    }
  }
}
