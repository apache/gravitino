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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.dto.responses.OAuth2TokenResponse;
import org.apache.gravitino.json.JsonUtils;

// Referred from Apache Iceberg's OAuth2Util implementation
// core/src/main/java/org/apache/iceberg/rest/OAuth2Util.java
class OAuth2ClientUtil {

  public static final String SCOPE = "scope";

  // Existing credentials are split into at most two parts, so a client secret may contain ':'. Do
  // not change this limit: doing so would silently change the meaning of existing
  // client_id:client_secret credentials whose secret contains ':'.
  private static final Splitter CREDENTIAL_SPLITTER = Splitter.on(":").limit(2).trimResults();

  private static final String GRANT_TYPE = "grant_type";
  private static final String CLIENT_CREDENTIALS = "client_credentials";

  // The password grant credential is
  // "password:<client_id>:<client_secret>:<username>:<password>". The explicit prefix selects the
  // grant instead of inferring it from the number of parts, which would silently turn an existing
  // credential into a password grant.
  private static final String PASSWORD_GRANT = "password";
  private static final String PASSWORD_GRANT_PREFIX = PASSWORD_GRANT + ":";
  // Only the password, which is the last part, may contain ':'.
  private static final Splitter PASSWORD_GRANT_SPLITTER = Splitter.on(":").limit(4);

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
      String path,
      OAuth2ClientAuthenticationMethod authenticationMethod) {
    List<String> scopes = scope != null ? ImmutableList.of(scope) : ImmutableList.of();

    Pair<Map<String, String>, Map<String, String>> request =
        tokenRequest(credential, scopes, authenticationMethod);

    OAuth2TokenResponse response =
        client.postForm(
            path,
            request.getLeft(),
            OAuth2TokenResponse.class,
            mergeHeaders(headers, request.getRight()),
            ErrorHandlers.oauthErrorHandler());
    response.validate();

    return response;
  }

  private static Map<String, String> mergeHeaders(
      Map<String, String> original, Map<String, String> additional) {
    Map<String, String> headers = new HashMap<>(original);
    headers.putAll(additional);
    return headers;
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

    private boolean isPasswordGrant() {
      return username != null;
    }
  }

  private static Credential parseCredential(String credential) {
    Preconditions.checkArgument(credential != null, "Invalid credential: null");
    if (credential.startsWith(PASSWORD_GRANT_PREFIX)) {
      return parsePasswordGrantCredential(credential.substring(PASSWORD_GRANT_PREFIX.length()));
    }

    List<String> parts = CREDENTIAL_SPLITTER.splitToList(credential);
    switch (parts.size()) {
      case 2:
        // client ID and client secret
        return new Credential(parts.get(0), parts.get(1), null, null);
      case 1:
        // client secret
        return new Credential(null, parts.get(0), null, null);
      default:
        // this should never happen because the credential splitter is limited to 2
        throw new IllegalArgumentException(
            "Invalid credential: expected client_secret or client_id:client_secret");
    }
  }

  private static Credential parsePasswordGrantCredential(String credential) {
    List<String> parts = PASSWORD_GRANT_SPLITTER.splitToList(credential);
    Preconditions.checkArgument(
        parts.size() == 4,
        "Invalid credential: expected "
            + PASSWORD_GRANT_PREFIX
            + "<client_id>:<client_secret>:<username>:<password>");

    String clientId = parts.get(0).trim();
    String clientSecret = parts.get(1).trim();
    String username = parts.get(2).trim();
    // Do not trim the password: leading and trailing whitespace is significant.
    String password = parts.get(3);

    Preconditions.checkArgument(
        !username.isEmpty(), "Invalid credential: username is required for the password grant");
    Preconditions.checkArgument(
        !password.isEmpty(), "Invalid credential: password is required for the password grant");
    Preconditions.checkArgument(
        !clientSecret.isEmpty(),
        "Invalid credential: client secret is required for the password grant");

    return new Credential(clientId.isEmpty() ? null : clientId, clientSecret, username, password);
  }

  private static Pair<Map<String, String>, Map<String, String>> tokenRequest(
      String credential,
      List<String> scopes,
      OAuth2ClientAuthenticationMethod authenticationMethod) {
    Preconditions.checkNotNull(authenticationMethod, "authenticationMethod cannot be null");
    Credential parsedCredential = parseCredential(credential);

    switch (authenticationMethod) {
      case CLIENT_SECRET_BASIC:
        return buildClientSecretBasicRequest(parsedCredential, scopes);
      case CLIENT_SECRET_POST:
        return buildClientCredentialRequest(parsedCredential, scopes);
      default:
        throw new IllegalArgumentException(
            "Unsupported OAuth2 client authentication method: " + authenticationMethod);
    }
  }

  private static Pair<Map<String, String>, Map<String, String>> buildClientSecretBasicRequest(
      Credential credential, List<String> scopes) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(credential.clientId),
        "Client ID is required for client_secret_basic authentication");

    String encodedClientId;
    String encodedClientSecret;
    try {
      encodedClientId = URLEncoder.encode(credential.clientId, StandardCharsets.UTF_8.name());
      encodedClientSecret =
          URLEncoder.encode(credential.clientSecret, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("UTF-8 encoding is unavailable", e);
    }

    String encoded =
        Base64.getEncoder()
            .encodeToString(
                (encodedClientId + ":" + encodedClientSecret).getBytes(StandardCharsets.UTF_8));

    Map<String, String> requestHeaders = new HashMap<>();
    requestHeaders.put("Authorization", "Basic " + encoded);

    ImmutableMap.Builder<String, String> formData = ImmutableMap.builder();
    formData.put(GRANT_TYPE, credential.isPasswordGrant() ? PASSWORD_GRANT : CLIENT_CREDENTIALS);
    if (credential.isPasswordGrant()) {
      formData.put(USERNAME, credential.username);
      formData.put(PASSWORD, credential.password);
    }
    formData.put(SCOPE, toScope(scopes));

    return Pair.of(formData.build(), requestHeaders);
  }

  private static Pair<Map<String, String>, Map<String, String>> buildClientCredentialRequest(
      Credential credential, List<String> scopes) {
    ImmutableMap.Builder<String, String> formData = ImmutableMap.builder();
    formData.put(GRANT_TYPE, credential.isPasswordGrant() ? PASSWORD_GRANT : CLIENT_CREDENTIALS);
    if (credential.clientId != null) {
      formData.put(CLIENT_ID, credential.clientId);
    }
    formData.put(CLIENT_SECRET, credential.clientSecret);
    if (credential.isPasswordGrant()) {
      formData.put(USERNAME, credential.username);
      formData.put(PASSWORD, credential.password);
    }
    formData.put(SCOPE, toScope(scopes));

    return Pair.of(formData.build(), ImmutableMap.of());
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
