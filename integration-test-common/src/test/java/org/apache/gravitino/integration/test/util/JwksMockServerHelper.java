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
package org.apache.gravitino.integration.test.util;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/** In-process JWKS + OAuth2 token mock server for integration tests. */
public class JwksMockServerHelper implements Closeable {

  private final HttpServer httpServer;
  private final RSAKey rsaKey;
  private final int port;

  @Nullable private volatile Supplier<String> tokenSupplier;
  private final Map<String, String> userTokens = new ConcurrentHashMap<>();
  @Nullable private volatile String fallbackToken;

  private JwksMockServerHelper(HttpServer httpServer, RSAKey rsaKey, int port) {
    this.httpServer = httpServer;
    this.rsaKey = rsaKey;
    this.port = port;
  }

  /** Returns the RSA key pair used for signing JWTs. */
  public RSAKey rsaKey() {
    return rsaKey;
  }

  /** Returns the port the mock server is listening on. */
  public int port() {
    return port;
  }

  /** Returns the base URI of the mock server, e.g. {@code http://localhost:12345}. */
  public String baseUri() {
    return "http://localhost:" + port;
  }

  /** Returns the JWKS endpoint URI, e.g. {@code http://localhost:12345/jwks}. */
  public String jwksUri() {
    return baseUri() + "/jwks";
  }

  /** Configures single-token mode: every {@code /token} request returns the supplier's token. */
  public void setTokenSupplier(Supplier<String> supplier) {
    this.tokenSupplier = supplier;
  }

  /** Registers a pre-minted JWT for a specific {@code client_id} (multi-user mode). */
  public void registerUserToken(String clientId, String token) {
    userTokens.put(clientId, token);
  }

  /** Sets the fallback token when {@code client_id} is not found in the user token map. */
  public void setFallbackToken(String token) {
    this.fallbackToken = token;
  }

  /** Stops the mock HTTP server. */
  @Override
  public void close() throws IOException {
    httpServer.stop(0);
  }

  /** Mints a compact RS256-signed JWT using this helper's RSA key. */
  public String mintToken(String subject, String audience, Instant expiry) throws Exception {
    return mintToken(rsaKey, subject, audience, expiry);
  }

  /** Mints a compact RS256-signed JWT using the specified RSA key. */
  @SuppressWarnings("JavaUtilDate")
  public static String mintToken(RSAKey key, String subject, String audience, Instant expiry)
      throws Exception {
    JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .subject(subject)
            .audience(audience)
            .issueTime(Date.from(Instant.now()))
            .expirationTime(Date.from(expiry))
            .build();
    SignedJWT jwt =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claims);
    jwt.sign(new RSASSASigner(key));
    return jwt.serialize();
  }

  /** Builds the JSON body returned by the mock {@code /token} endpoint. */
  public static String buildTokenResponseJson(String accessToken) {
    return "{\"access_token\":\""
        + accessToken
        + "\",\"token_type\":\"bearer\",\"expires_in\":86400}";
  }

  /** Parses a single parameter from a URL-encoded form body. */
  static String parseFormParam(String body, String key) {
    for (String pair : body.split("&")) {
      String[] kv = pair.split("=", 2);
      if (kv.length == 2 && URLDecoder.decode(kv[0], StandardCharsets.UTF_8).equals(key)) {
        return URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
      }
    }
    return null;
  }

  /** Creates and starts a new mock server with a freshly generated RSA key pair on a random port. */
  public static JwksMockServerHelper create(String keyId) throws Exception {
    RSAKey rsaKey = new RSAKeyGenerator(2048).keyID(keyId).generate();
    String jwksJson = new JWKSet(rsaKey.toPublicJWK()).toString();

    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);

    JwksMockServerHelper helper =
        new JwksMockServerHelper(server, rsaKey, server.getAddress().getPort());

    // /jwks – serves the JWKS public key document.
    server.createContext(
        "/jwks",
        exchange -> {
          byte[] body = jwksJson.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.getResponseBody().close();
        });

    // /token – mock OAuth2 client_credentials endpoint.
    server.createContext(
        "/token",
        exchange -> {
          String postBody =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);

          String token;
          Supplier<String> supplier = helper.tokenSupplier;
          if (supplier != null) {
            // Single-token mode: use the supplier.
            token = supplier.get();
          } else {
            // Multi-user mode: look up client_id.
            String clientId = parseFormParam(postBody, "client_id");
            String fb = helper.fallbackToken;
            token =
                clientId != null && helper.userTokens.containsKey(clientId)
                    ? helper.userTokens.get(clientId)
                    : (fb != null ? fb : "");
          }

          byte[] resp = buildTokenResponseJson(token).getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, resp.length);
          exchange.getResponseBody().write(resp);
          exchange.getResponseBody().close();
        });

    server.start();
    return helper;
  }
}
