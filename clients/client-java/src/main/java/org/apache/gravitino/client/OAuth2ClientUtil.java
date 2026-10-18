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
  private static final Splitter CREDENTIAL_SPLITTER = Splitter.on(":").limit(2).trimResults();
  private static final String GRANT_TYPE = "grant_type";
  private static final String CLIENT_CREDENTIALS = "client_credentials";

  private static final String CLIENT_ID = "client_id";
  private static final String CLIENT_SECRET = "client_secret";

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
        clientCredentialsRequest(credential, scopes, authenticationMethod);

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

  private static Pair<String, String> parseCredential(String credential) {
    Preconditions.checkArgument(credential != null, "Invalid credential: null");
    List<String> parts = CREDENTIAL_SPLITTER.splitToList(credential);
    switch (parts.size()) {
      case 2:
        // client ID and client secret
        return Pair.of(parts.get(0), parts.get(1));
      case 1:
        // client secret
        return Pair.of(null, parts.get(0));
      default:
        // this should never happen because the credential splitter is limited to 2
        throw new IllegalArgumentException("Invalid credential: " + credential);
    }
  }

  private static Pair<Map<String, String>, Map<String, String>> clientCredentialsRequest(
      String credential,
      List<String> scopes,
      OAuth2ClientAuthenticationMethod authenticationMethod) {
    Pair<String, String> credentialPair = parseCredential(credential);

    if (authenticationMethod == OAuth2ClientAuthenticationMethod.CLIENT_SECRET_BASIC) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(credentialPair.getLeft()),
          "Client ID is required for client_secret_basic authentication");

      String encodedClientId;
      String encodedClientSecret;

      try {
        encodedClientId =
            URLEncoder.encode(credentialPair.getLeft(), StandardCharsets.UTF_8.name());
        encodedClientSecret =
            URLEncoder.encode(credentialPair.getRight(), StandardCharsets.UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException("UTF-8 encoding is unavailable", e);
      }

      String encoded =
          Base64.getEncoder()
              .encodeToString(
                  (encodedClientId + ":" + encodedClientSecret).getBytes(StandardCharsets.UTF_8));

      Map<String, String> requestHeaders = new HashMap<>();
      requestHeaders.put("Authorization", "Basic " + encoded);

      Map<String, String> formData =
          ImmutableMap.<String, String>builder()
              .put(GRANT_TYPE, CLIENT_CREDENTIALS)
              .put(SCOPE, toScope(scopes))
              .build();

      return Pair.of(formData, requestHeaders);
    }

    return Pair.of(
        clientCredentialsRequest(credentialPair.getLeft(), credentialPair.getRight(), scopes),
        ImmutableMap.of());
  }

  private static Map<String, String> clientCredentialsRequest(
      String clientId, String clientSecret, List<String> scopes) {
    ImmutableMap.Builder<String, String> formData = ImmutableMap.builder();
    formData.put(GRANT_TYPE, CLIENT_CREDENTIALS);
    if (clientId != null) {
      formData.put(CLIENT_ID, clientId);
    }
    formData.put(CLIENT_SECRET, clientSecret);
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
