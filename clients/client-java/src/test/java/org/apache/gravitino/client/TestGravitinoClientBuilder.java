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
package org.apache.gravitino.client;

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

  @Test
  public void testGravitinoClientSimpleAuthWithUserName() {
    String userName = "test_user";
    try (MockGravitinoClient client =
        MockGravitinoClient.builder("http://127.0.0.1").withSimpleAuth(userName).build()) {
      Assertions.assertArrayEquals(
          new SimpleTokenProvider(userName).getTokenData(),
          client.getAuthDataProvider().getTokenData());
    }

    try (MockGravitinoClient client =
        MockGravitinoClient.builder("http://127.0.0.1").withSimpleAuth().build()) {
      Assertions.assertArrayEquals(
          new SimpleTokenProvider().getTokenData(), client.getAuthDataProvider().getTokenData());
    }
  }

  @Test
  public void testGravitinoClientProperties() {
    Map<String, String> properties =
        ImmutableMap.of(
            "gravitino.client.connectionTimeoutMs", "10", "gravitino.client.socketTimeoutMs", "10");
    try (MockGravitinoClient client =
        MockGravitinoClient.builder("http://127.0.0.1").withClientConfig(properties).build()) {
      Assertions.assertEquals(properties, client.getProperties());
    }

    try (MockGravitinoClient client = MockGravitinoClient.builder("http://127.0.0.1").build()) {
      Assertions.assertEquals(ImmutableMap.of(), client.getProperties());
    }

    try (MockGravitinoClient client =
        MockGravitinoClient.builder("http://127.0.0.1").withClientConfig(null).build()) {
      Assertions.assertEquals(ImmutableMap.of(), client.getProperties());
    }
  }
}

class MockGravitinoClient extends GravitinoClientBase {

  private Map<String, String> headers;

  private AuthDataProvider authDataProvider;

  private Map<String, String> properties;

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param authDataProvider The provider of the data which is used for authentication.
   * @param headers The base header of the Gravitino API.
   * @param properties A map of properties (key-value pairs) used to configure the HTTP client.
   */
  private MockGravitinoClient(
      String uri,
      AuthDataProvider authDataProvider,
      Map<String, String> headers,
      Map<String, String> properties) {
    super(uri, authDataProvider, false, headers, properties);
    this.headers = headers;
    this.authDataProvider = authDataProvider;
    this.properties = properties;
  }

  Map<String, String> getHeaders() {
    return headers;
  }

  AuthDataProvider getAuthDataProvider() {
    return authDataProvider;
  }

  Map<String, String> getProperties() {
    return properties;
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
      return new MockGravitinoClient(uri, authDataProvider, headers, properties);
    }
  }
}
