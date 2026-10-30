/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.dto.responses.OAuth2TokenResponse;
import org.apache.gravitino.json.JsonUtils;

// Referred from Apache Iceberg's OAuth2Util implementation
// core/src/main/java/org/apache/iceberg/rest/OAuth2Util.java
class OAuth2ClientUtil {

  public static final String SCOPE = "scope";
  private static final Splitter CREDENTIAL_SPLITTER = Splitter.on(":").limit(4).trimResults();
  private static final String GRANT_TYPE = "grant_type";
  private static final String CLIENT_CREDENTIALS = "client_credentials";
  private static final String PASSWORD_GRANT = "password";

  private static final String CLIENT_ID = "client_id";
  private static final String CLIENT_SECRET = "client_secret";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  // error type constants
  public static final String INVALID_REQUEST_ERROR = "invalid_request";
  public static final String INVALID_CLIENT_ERROR = "invalid_client";
  public static final String INVALID_GRANT_ERROR = "invalid_grant";
  public static final String UNAUTHORIZED_CLIENT_ERROR = "unauthorized_client";
  public static final String UNSUPPORTED_GRANT_TYPE_ERROR = "unsupported_grant_type";
  public static final String INVALID_SCOPE_ERROR = "invalid_scope";

  private OAuth2ClientUtil() {}

  public static OAuth2TokenResponse fetchToken(
      RESTClient client,
      Map<String, String> headers,
      String credential,
      String scope,
      String path) {
    List<String> scopes = scope != null ? ImmutableList.of(scope) : ImmutableList.of();
    Credential parsed = parseCredential(credential);
    Map<String, String> request =
        parsed.username != null
            ? passwordGrantRequest(parsed, scopes)
            : clientCredentialsRequest(parsed, scopes);

    OAuth2TokenResponse response =
        client.postForm(
            path, request, OAuth2TokenResponse.class, headers, ErrorHandlers.oauthErrorHandler());
    response.validate();

    return response;
  }

  private static final class Credential {
    private final String clientId;
    private final String clientSecret;
    private final String username;
    private final String password;

    private Credential(String clientId, String clientSecret, String username, String password) {
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.username = username;
      this.password = password;
    }
  }

  private static Credential parseCredential(String credential) {
    Preconditions.checkArgument(credential != null, "Invalid credential: null");
    List<String> parts = CREDENTIAL_SPLITTER.splitToList(credential);
    switch (parts.size()) {
      case 4:
        // client ID, client secret, username and password, which switches to the password grant.
        // The client ID can be left empty if the OAuth2 server does not require one.
        Preconditions.checkArgument(
            !parts.get(2).isEmpty() && !parts.get(3).isEmpty(),
            "Invalid credential: username and password are required for the password grant");
        return new Credential(
            parts.get(0).isEmpty() ? null : parts.get(0), parts.get(1), parts.get(2), parts.get(3));
      case 2:
        // client ID and client secret
        return new Credential(parts.get(0), parts.get(1), null, null);
      case 1:
        // client secret
        return new Credential(null, parts.get(0), null, null);
      default:
        throw new IllegalArgumentException(
            "Invalid credential: expected client_secret, client_id:client_secret or "
                + "client_id:client_secret:username:password");
    }
  }

  private static Map<String, String> clientCredentialsRequest(
      Credential credential, List<String> scopes) {
    ImmutableMap.Builder<String, String> formData = ImmutableMap.builder();
    formData.put(GRANT_TYPE, CLIENT_CREDENTIALS);
    if (credential.clientId != null) {
      formData.put(CLIENT_ID, credential.clientId);
    }
    formData.put(CLIENT_SECRET, credential.clientSecret);
    formData.put(SCOPE, toScope(scopes));

    return formData.build();
  }

  private static Map<String, String> passwordGrantRequest(
      Credential credential, List<String> scopes) {
    ImmutableMap.Builder<String, String> formData = ImmutableMap.builder();
    formData.put(GRANT_TYPE, PASSWORD_GRANT);
    if (credential.clientId != null) {
      formData.put(CLIENT_ID, credential.clientId);
    }
    formData.put(CLIENT_SECRET, credential.clientSecret);
    formData.put(USERNAME, credential.username);
    formData.put(PASSWORD, credential.password);
    formData.put(SCOPE, toScope(scopes));

    return formData.build();
  }

  /**
   * If the token is a JWT, extracts the expiration timestamp from the ext claim or null.
   *
   * @param token a token String
   * @return The epoch millisecond the token expires at or null if it's not a valid JWT.
   */
  static Long expiresAtMillis(String token) {
    if (null == token) {
      return null;
    }

    List<String> parts = Splitter.on('.').splitToList(token);
    if (parts.size() != 3) {
      return null;
    }

    JsonNode node;
    try {
      node =
          ObjectMapperProvider.objectMapper().readTree(Base64.getUrlDecoder().decode(parts.get(1)));
    } catch (IOException e) {
      return null;
    }

    String property = "exp";
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }
    return TimeUnit.SECONDS.toMillis(JsonUtils.getLong(property, node));
  }

  private static final Joiner SCOPE_JOINER = Joiner.on(" ");

  public static String toScope(Iterable<String> scopes) {
    return SCOPE_JOINER.join(scopes);
  }
}
