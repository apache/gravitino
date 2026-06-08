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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import org.junit.jupiter.api.Test;

public class TestMockOAuthTokenServer {

  @Test
  public void testServesSignedTokenForSubject() throws Exception {
    KeyPair keyPair = MockOAuthTokenServer.newRsaKeyPair();
    try (MockOAuthTokenServer server =
        new MockOAuthTokenServer(keyPair, "test-audience", "service-user")) {
      server.start();

      HttpResponse<String> resp =
          HttpClient.newHttpClient()
              .send(
                  HttpRequest.newBuilder(URI.create(server.tokenEndpoint()))
                      .header("Content-Type", "application/x-www-form-urlencoded")
                      .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials"))
                      .build(),
                  HttpResponse.BodyHandlers.ofString());

      assertEquals(200, resp.statusCode());
      String accessToken = server.lastIssuedToken();
      assertTrue(resp.body().contains(accessToken));

      // jjwt is resolved to 0.13.x on this module's test classpath (forced up by Trino's
      // transitive dependencies). In 0.13.x, Jwts.parserBuilder() was renamed to Jwts.parser(),
      // setSigningKey(PublicKey) became verifyWith(PublicKey), and parseClaimsJws(...).getBody()
      // became parseSignedClaims(...).getPayload(). getAudience() now returns a Set<String>.
      Claims claims =
          Jwts.parser()
              .verifyWith(keyPair.getPublic())
              .build()
              .parseSignedClaims(accessToken)
              .getPayload();
      assertEquals("service-user", claims.getSubject());
      assertTrue(claims.getAudience().contains("test-audience"));
    }
  }
}
