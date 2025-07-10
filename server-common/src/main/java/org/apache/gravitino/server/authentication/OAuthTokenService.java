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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth Token Service
 *
 * <p>Handles token exchange operations for OAuth 2.0 Authorization Code Flow. This service
 * exchanges authorization codes for access tokens from OAuth providers.
 */
public class OAuthTokenService {

  private static final Logger LOG = LoggerFactory.getLogger(OAuthTokenService.class);

  private final OAuthAuthorizationCodeConfig config;

  public OAuthTokenService(OAuthAuthorizationCodeConfig config) {
    this.config = config;
  }

  /**
   * Exchange authorization code for access token
   *
   * <p>This method implements the token exchange step of OAuth 2.0 Authorization Code Flow. It
   * sends a POST request to the OAuth provider's token endpoint with the authorization code and
   * receives an access token in response.
   *
   * @param authorizationCode The authorization code received from the OAuth provider
   * @return TokenResponse containing access token and other token information
   * @throws Exception if token exchange fails
   */
  public TokenResponse exchangeCodeForToken(String authorizationCode) throws Exception {
    LOG.info("Exchanging authorization code for access token");

    if (Strings.isNullOrEmpty(authorizationCode)) {
      throw new IllegalArgumentException("Authorization code cannot be null or empty");
    }

    // Build token exchange request
    String requestBody = buildTokenExchangeRequest(authorizationCode);

    // Send HTTP POST request to token endpoint
    HttpURLConnection connection = null;
    try {
      URL url = new URL(config.getTokenUrl());
      connection = (HttpURLConnection) url.openConnection();

      // Configure HTTP request
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      connection.setRequestProperty("Accept", "application/json");
      connection.setDoOutput(true);

      // Send request body
      try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
        wr.writeBytes(requestBody);
        wr.flush();
      }

      // Read response
      int responseCode = connection.getResponseCode();
      LOG.debug("Token exchange response code: {}", responseCode);

      if (responseCode == HttpURLConnection.HTTP_OK) {
        // Success - parse token response
        String responseBody = readResponse(connection.getInputStream());
        LOG.debug("Token exchange successful");
        return parseTokenResponse(responseBody);
      } else {
        // Error - read error response
        String errorResponse = readResponse(connection.getErrorStream());
        LOG.error(
            "Token exchange failed with response code: {}, error: {}", responseCode, errorResponse);
        throw new Exception("Token exchange failed: " + errorResponse);
      }

    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  /** Build the token exchange request body */
  private String buildTokenExchangeRequest(String authorizationCode) throws Exception {
    StringBuilder requestBody = new StringBuilder();

    requestBody.append("grant_type=authorization_code");
    requestBody
        .append("&client_id=")
        .append(URLEncoder.encode(config.getClientId(), StandardCharsets.UTF_8.name()));
    requestBody
        .append("&client_secret=")
        .append(URLEncoder.encode(config.getClientSecret(), StandardCharsets.UTF_8.name()));
    requestBody
        .append("&code=")
        .append(URLEncoder.encode(authorizationCode, StandardCharsets.UTF_8.name()));
    requestBody
        .append("&redirect_uri=")
        .append(URLEncoder.encode(config.getRedirectUri(), StandardCharsets.UTF_8.name()));

    // Add scope if configured
    if (!Strings.isNullOrEmpty(config.getScope())) {
      requestBody
          .append("&scope=")
          .append(URLEncoder.encode(config.getScope(), StandardCharsets.UTF_8.name()));
    }

    return requestBody.toString();
  }

  /** Read HTTP response body */
  private String readResponse(java.io.InputStream inputStream) throws Exception {
    if (inputStream == null) {
      return "";
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        response.append(line);
      }
    }
    return response.toString();
  }

  /** Parse token response JSON */
  private TokenResponse parseTokenResponse(String responseBody) throws Exception {
    LOG.debug("Parsing token response: {}", responseBody);

    // For now, we'll use simple JSON parsing
    // In a production environment, you might want to use Jackson or another JSON library
    TokenResponse tokenResponse = new TokenResponse();

    // Extract access_token
    String accessToken = extractJsonValue(responseBody, "access_token");
    if (Strings.isNullOrEmpty(accessToken)) {
      throw new Exception("No access_token found in token response");
    }
    tokenResponse.setAccessToken(accessToken);

    // Extract optional fields
    tokenResponse.setTokenType(extractJsonValue(responseBody, "token_type"));
    tokenResponse.setRefreshToken(extractJsonValue(responseBody, "refresh_token"));
    tokenResponse.setScope(extractJsonValue(responseBody, "scope"));

    // Extract expires_in
    String expiresIn = extractJsonValue(responseBody, "expires_in");
    if (!Strings.isNullOrEmpty(expiresIn)) {
      try {
        tokenResponse.setExpiresIn(Integer.parseInt(expiresIn));
      } catch (NumberFormatException e) {
        LOG.warn("Failed to parse expires_in: {}", expiresIn);
      }
    }

    return tokenResponse;
  }

  /**
   * Simple JSON value extraction Note: This is a basic implementation. In production, consider
   * using a proper JSON library.
   */
  private String extractJsonValue(String json, String key) {
    String searchKey = "\"" + key + "\"";
    int keyIndex = json.indexOf(searchKey);
    if (keyIndex == -1) {
      return null;
    }

    int colonIndex = json.indexOf(":", keyIndex);
    if (colonIndex == -1) {
      return null;
    }

    int valueStart = colonIndex + 1;
    while (valueStart < json.length()
        && (json.charAt(valueStart) == ' ' || json.charAt(valueStart) == '\t')) {
      valueStart++;
    }

    if (valueStart >= json.length()) {
      return null;
    }

    int valueEnd;
    if (json.charAt(valueStart) == '"') {
      // String value
      valueStart++; // Skip opening quote
      valueEnd = json.indexOf("\"", valueStart);
      if (valueEnd == -1) {
        return null;
      }
    } else {
      // Number or other value
      valueEnd = valueStart;
      while (valueEnd < json.length()
          && json.charAt(valueEnd) != ','
          && json.charAt(valueEnd) != '}'
          && json.charAt(valueEnd) != ' '
          && json.charAt(valueEnd) != '\t'
          && json.charAt(valueEnd) != '\n') {
        valueEnd++;
      }
    }

    if (valueEnd <= valueStart) {
      return null;
    }

    return json.substring(valueStart, valueEnd);
  }

  /** Token response from OAuth provider */
  public static class TokenResponse {
    @JsonProperty("access_token")
    private String accessToken;

    @JsonProperty("token_type")
    private String tokenType;

    @JsonProperty("expires_in")
    private Integer expiresIn;

    @JsonProperty("refresh_token")
    private String refreshToken;

    @JsonProperty("scope")
    private String scope;

    public String getAccessToken() {
      return accessToken;
    }

    public void setAccessToken(String accessToken) {
      this.accessToken = accessToken;
    }

    public String getTokenType() {
      return tokenType;
    }

    public void setTokenType(String tokenType) {
      this.tokenType = tokenType;
    }

    public Integer getExpiresIn() {
      return expiresIn;
    }

    public void setExpiresIn(Integer expiresIn) {
      this.expiresIn = expiresIn;
    }

    public String getRefreshToken() {
      return refreshToken;
    }

    public void setRefreshToken(String refreshToken) {
      this.refreshToken = refreshToken;
    }

    public String getScope() {
      return scope;
    }

    public void setScope(String scope) {
      this.scope = scope;
    }
  }
}
