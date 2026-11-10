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
package org.apache.gravitino.iceberg.integration.test.trino;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.jsonwebtoken.Jwts;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Date;

/**
 * A minimal OAuth2 token endpoint for tests. Answers any client-credentials POST with a freshly
 * signed RS256 JWT whose subject and audience are fixed at construction. Bound to 127.0.0.1 so a
 * deploy-mode Gravitino server subprocess on the same host can reach it.
 *
 * <p>This module's test classpath resolves jjwt to 0.13.x (forced up by Trino's transitive
 * dependencies), so the non-deprecated 0.13.x signing API is used here.
 */
public final class MockOAuthTokenServer implements AutoCloseable {

  private static final String TOKEN_PATH = "/oauth2/token";

  private final HttpServer httpServer;
  private final KeyPair keyPair;
  private final String audience;
  private final String subject;
  private volatile String lastIssuedToken;

  /**
   * Creates a token server bound to an ephemeral port on 127.0.0.1.
   *
   * @param keyPair the RS256 key pair used to sign issued tokens
   * @param audience the audience claim placed on every issued token
   * @param subject the subject claim placed on every issued token
   * @throws IOException if the underlying HTTP server cannot be created
   */
  public MockOAuthTokenServer(KeyPair keyPair, String audience, String subject) throws IOException {
    this.keyPair = keyPair;
    this.audience = audience;
    this.subject = subject;
    this.httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    this.httpServer.createContext(TOKEN_PATH, this::handle);
  }

  /**
   * Generates an RS256 key pair suitable for signing/validating the test tokens.
   *
   * @return a freshly generated RSA key pair
   */
  public static KeyPair newRsaKeyPair() {
    return Jwts.SIG.RS256.keyPair().build();
  }

  /** Starts the embedded HTTP server. */
  public void start() {
    httpServer.start();
  }

  /**
   * Returns the ephemeral port the server is listening on.
   *
   * @return the bound TCP port
   */
  public int port() {
    return httpServer.getAddress().getPort();
  }

  /**
   * Returns the base server URI, e.g. {@code http://127.0.0.1:34567} — pass as {@code
   * GRAVITINO_OAUTH2_SERVER_URI}.
   *
   * @return the base server URI
   */
  public String serverUri() {
    return "http://127.0.0.1:" + port();
  }

  /**
   * Returns the token path, e.g. {@code /oauth2/token} — pass as {@code
   * GRAVITINO_OAUTH2_TOKEN_PATH}.
   *
   * @return the token path
   */
  public String tokenPath() {
    return TOKEN_PATH;
  }

  /**
   * Returns the fully qualified token endpoint URL.
   *
   * @return the token endpoint URL
   */
  public String tokenEndpoint() {
    return serverUri() + TOKEN_PATH;
  }

  /**
   * Returns the most recently issued token, or {@code null} if none has been issued yet.
   *
   * @return the last issued token
   */
  public String lastIssuedToken() {
    return lastIssuedToken;
  }

  private void handle(HttpExchange exchange) throws IOException {
    String token =
        Jwts.builder()
            .subject(subject)
            .audience()
            .add(audience)
            .and()
            .expiration(new Date(System.currentTimeMillis() + 60 * 60 * 1000))
            .signWith(keyPair.getPrivate(), Jwts.SIG.RS256)
            .compact();
    this.lastIssuedToken = token;
    String body =
        "{\"access_token\":\"" + token + "\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  @Override
  public void close() {
    httpServer.stop(0);
  }
}
