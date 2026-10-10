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
package org.apache.gravitino.server.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.Filter;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Config;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.ServerHealth;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Verifies authentication failures are recorded across the real HTTP filter chain. */
class TestAuthenticationOutOfMemoryHttp {
  @ParameterizedTest
  @ValueSource(strings = {"direct", "wrapped", "unauthorized", "ordinary", "ordinary-unauthorized"})
  void recordsOomBeforeConvertingAuthenticationErrors(String kind) throws Exception {
    ServerHealth health = new ServerHealth();
    AtomicBoolean recordedBeforeConversion = new AtomicBoolean();
    Authenticator authenticator = mock(Authenticator.class);
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    Throwable failure;
    switch (kind) {
      case "direct":
        failure = new OutOfMemoryError("Metaspace");
        break;
      case "wrapped":
        failure = new IllegalStateException(new OutOfMemoryError("Java heap space"));
        break;
      case "unauthorized":
        failure =
            new UnauthorizedException(new OutOfMemoryError("Metaspace"), "authentication failed");
        break;
      case "ordinary-unauthorized":
        failure = new UnauthorizedException("invalid credentials");
        break;
      default:
        failure = new IllegalStateException("ordinary failure");
    }
    when(authenticator.authenticateToken(any())).thenThrow(failure);
    int port = RESTUtils.findAvailablePort(0, 0);
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, port);
    JettyServer server =
        new JettyServer() {
          /** {@inheritDoc} */
          @Override
          protected Filter createAuthenticationFilter(boolean includeErrorStackTrace) {
            return new AuthenticationFilter(Collections.singletonList(authenticator)) {
              /** {@inheritDoc} */
              @Override
              protected void sendAuthErrorResponse(
                  HttpServletResponse response, Exception exception) throws IOException {
                recordedBeforeConversion.set(health.hasOutOfMemoryError());
                super.sendAuthErrorResponse(response, exception);
              }
            };
          }
        };
    try {
      try (MockedStatic<ServerHealth> state = mockStatic(ServerHealth.class)) {
        state.when(ServerHealth::getInstance).thenReturn(health);
        server.initialize(JettyServerConfig.fromConfig(config), "authentication-oom-test", false);
        server.addSystemFilters("/*");
      }
      server.start();
      HttpResponse<String> response =
          HttpClient.newHttpClient()
              .send(
                  HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/test"))
                      .header("Authorization", "test")
                      .GET()
                      .build(),
                  HttpResponse.BodyHandlers.ofString());
      assertEquals(kind.contains("unauthorized") ? 401 : 500, response.statusCode());
      assertEquals(!kind.startsWith("ordinary"), health.hasOutOfMemoryError());
      if (!kind.equals("direct")) {
        assertEquals(!kind.startsWith("ordinary"), recordedBeforeConversion.get());
      }
    } finally {
      server.stop();
    }
  }
}
