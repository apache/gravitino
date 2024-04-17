/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoClientBuilder {
  @Test
  public void testGravitinoClientHeaders() {
    Map<String, String> headers = ImmutableMap.of("k1", "v1");
    try (MockGravitinoClient client =
        MockGravitinoClient.builder("http://127.0.0.1").withHeaders(headers).build()) {
      Assertions.assertEquals(headers, client.getHeaders());
    }

    try (MockGravitinoClient client1 = MockGravitinoClient.builder("http://127.0.0.1").build()) {
      Assertions.assertEquals(ImmutableMap.of(), client1.getHeaders());
    }

    try (MockGravitinoClient client1 =
        MockGravitinoClient.builder("http://127.0.0.1").withHeaders(null).build()) {
      Assertions.assertEquals(ImmutableMap.of(), client1.getHeaders());
    }
  }
}

class MockGravitinoClient extends GravitinoClientBase {

  private Map<String, String> headers;

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param authDataProvider The provider of the data which is used for authentication.
   * @param headers The base header of the Gravitino API.
   */
  private MockGravitinoClient(
      String uri, AuthDataProvider authDataProvider, Map<String, String> headers) {
    super(uri, authDataProvider, headers);
    this.headers = headers;
  }

  Map<String, String> getHeaders() {
    return headers;
  }

  /**
   * Creates a new builder for constructing a GravitinoClient.
   *
   * @param uri The base URI for the Gravitino API.
   * @return A new instance of the Builder class for constructing a GravitinoClient.
   */
  static MockGravitinoClientBuilder builder(String uri) {
    return new MockGravitinoClientBuilder(uri);
  }

  static class MockGravitinoClientBuilder extends GravitinoClientBase.Builder<MockGravitinoClient> {

    /**
     * The constructor for the Builder class.
     *
     * @param uri The base URI for the Gravitino API.
     */
    protected MockGravitinoClientBuilder(String uri) {
      super(uri);
    }

    @Override
    public MockGravitinoClient build() {
      return new MockGravitinoClient(uri, authDataProvider, headers);
    }
  }
}
