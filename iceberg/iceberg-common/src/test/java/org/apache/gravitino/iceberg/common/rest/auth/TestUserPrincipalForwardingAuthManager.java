/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.common.rest.auth;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.auth.AuthSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestUserPrincipalForwardingAuthManager {

  @Test
  public void testAuthenticateAddsAuthorizationHeaderFromUserPrincipal() throws Exception {
    UserPrincipalForwardingAuthManager manager = new UserPrincipalForwardingAuthManager("test");
    AuthSession session = manager.catalogSession(null, Collections.emptyMap());

    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://remote-iceberg:9001"))
            .method(HTTPRequest.HTTPMethod.GET)
            .path("v1/config")
            .build();

    HTTPRequest authenticated =
        PrincipalUtils.doAs(
            new UserPrincipal(
                "alice", AuthConstants.AUTHORIZATION_BEARER_HEADER + "jwt-token-abc"),
            () -> session.authenticate(request));

    Assertions.assertTrue(
        authenticated.headers().contains("Authorization"),
        "Expected Authorization header on outgoing Iceberg REST request");
    String authValue =
        authenticated.headers().entries("Authorization").iterator().next().value();
    Assertions.assertEquals(
        AuthConstants.AUTHORIZATION_BEARER_HEADER + "jwt-token-abc", authValue);
    manager.close();
  }

  @Test
  public void testAuthenticateWithoutAccessTokenThrows() throws Exception {
    UserPrincipalForwardingAuthManager manager = new UserPrincipalForwardingAuthManager("test");
    AuthSession session = manager.catalogSession(null, Map.of());

    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://remote-iceberg:9001"))
            .method(HTTPRequest.HTTPMethod.GET)
            .path("v1/config")
            .build();

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            PrincipalUtils.doAs(
                new UserPrincipal("bob"),
                () -> session.authenticate(request)));
    manager.close();
  }

  @Test
  public void testPutIfAbsentDoesNotOverrideExistingAuthorization() throws Exception {
    UserPrincipalForwardingAuthManager manager = new UserPrincipalForwardingAuthManager("test");
    AuthSession session = manager.catalogSession(null, Map.of());

    Map<String, String> headerMap = new HashMap<>();
    headerMap.put("Authorization", "Bearer existing");
    HTTPHeaders existing = HTTPHeaders.of(headerMap);
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://remote-iceberg:9001"))
            .method(HTTPRequest.HTTPMethod.GET)
            .path("v1/config")
            .headers(existing)
            .build();

    HTTPRequest authenticated =
        PrincipalUtils.doAs(
            new UserPrincipal(
                "alice", AuthConstants.AUTHORIZATION_BEARER_HEADER + "jwt-token-abc"),
            () -> session.authenticate(request));

    String authValue =
        authenticated.headers().entries("Authorization").iterator().next().value();
    Assertions.assertEquals("Bearer existing", authValue);
    manager.close();
  }
}
