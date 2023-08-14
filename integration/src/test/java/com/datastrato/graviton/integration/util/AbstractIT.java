/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.util;

import com.datastrato.graviton.client.GravitonClient;
import com.datastrato.graviton.client.HTTPClient;
import com.datastrato.graviton.client.RESTClient;
import com.datastrato.graviton.dto.responses.ErrorResponse;
import com.datastrato.graviton.rest.RESTRequest;
import com.datastrato.graviton.rest.RESTResponse;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(AbstractIT.class);
  private static RESTClient restClient;

  private static final int port = 8090;
  private static final String URI = String.format("http://127.0.0.1:%d", port);
  protected static GravitonClient client;

  @BeforeAll
  public static void startUp() {
    LOG.info("Starting up Graviton Server");
    GravitonITUtils.startGravitonServer();
    restClient = HTTPClient.builder(ImmutableMap.of()).uri(URI).build();

    client = GravitonClient.builder("http://127.0.0.1:" + port).build();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    restClient.close();
    GravitonITUtils.stopGravitonServer();
    LOG.info("Tearing down Graviton Server");
  }

  protected static <T extends RESTResponse> T doExecuteRequest(
      Method method,
      String path,
      RESTRequest body,
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
