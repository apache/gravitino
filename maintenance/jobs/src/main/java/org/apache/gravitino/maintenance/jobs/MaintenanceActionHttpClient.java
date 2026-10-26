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
package org.apache.gravitino.maintenance.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.OAuth2TokenResponse;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionMetricsRequest;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoAuthSettings;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.rest.RESTUtils;

/**
 * HTTP client for TMS Spark callbacks ({@code metrics/before}, {@code metrics/after}, {@code
 * finish}).
 *
 * <p>Uses {@link HttpURLConnection} only (Java 8, no {@code client-java}). Authentication follows
 * the same {@code --updater-options} keys as {@code builtin-iceberg-update-stats} ({@link
 * GravitinoAuthSettings}): {@code none}, {@code simple}, {@code basic}, and {@code oauth}.
 */
public final class MaintenanceActionHttpClient implements AutoCloseable {

  private static final String ACCEPT = "application/vnd.gravitino.v1+json";
  private static final String JSON = "application/json";
  private static final String FORM_URLENCODED = "application/x-www-form-urlencoded";
  private static final String SIMPLE_AUTH_DUMMY_PASSWORD = "dummy";
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 60_000;
  private static final int OAUTH_EXPIRY_SKEW_SECONDS = 30;
  private static final int DEFAULT_OAUTH_EXPIRES_IN_SECONDS = 300;
  private static final int ERROR_BODY_TRUNCATE = 512;

  private final ObjectMapper mapper;
  private final String baseUri;
  private final String metalakeName;
  private final GravitinoAuthSettings authSettings;

  @Nullable private String cachedBearer;
  private long bearerExpiresAtMillis;

  @VisibleForTesting
  MaintenanceActionHttpClient(
      ObjectMapper mapper,
      String baseUri,
      String metalakeName,
      GravitinoAuthSettings authSettings) {
    this.mapper = Preconditions.checkNotNull(mapper, "mapper");
    this.baseUri = trimTrailingSlash(Preconditions.checkNotNull(baseUri, "baseUri"));
    this.metalakeName = Preconditions.checkNotNull(metalakeName, "metalakeName");
    this.authSettings = Preconditions.checkNotNull(authSettings, "authSettings");
  }

  /**
   * Creates a client from updater-options (same shape as iceberg-update-stats).
   *
   * @param updaterOptions flat map from {@code --updater-options} JSON
   * @return open client
   */
  public static MaintenanceActionHttpClient fromUpdaterOptions(Map<String, String> updaterOptions) {
    Map<String, String> properties = normalizeUpdaterOptions(updaterOptions);
    String uri = properties.get(OptimizerConfig.GRAVITINO_URI);
    String metalake = properties.get(OptimizerConfig.GRAVITINO_METALAKE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(uri), "updater_options must contain gravitino_uri");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake), "updater_options must contain metalake");
    GravitinoAuthSettings auth = GravitinoAuthSettings.from(new OptimizerConfig(properties));
    return new MaintenanceActionHttpClient(
        JsonUtils.objectMapper(), uri.trim(), metalake.trim(), auth);
  }

  /**
   * Reports S0 (before-maintenance) snapshot metrics.
   *
   * @param jobId Gravitino job run id
   * @param request metrics payload
   */
  public void reportBeforeMetrics(long jobId, MaintenanceActionMetricsRequest request) {
    postJson(jobId, "metrics/before", request);
  }

  /**
   * Reports S1 (after-maintenance) snapshot metrics.
   *
   * @param jobId Gravitino job run id
   * @param request metrics payload
   */
  public void reportAfterMetrics(long jobId, MaintenanceActionMetricsRequest request) {
    postJson(jobId, "metrics/after", request);
  }

  /**
   * Closes the soft slot for one table without writing metrics.
   *
   * @param jobId Gravitino job run id
   * @param request finish payload
   */
  public void finish(long jobId, MaintenanceActionFinishRequest request) {
    postJson(jobId, "finish", request);
  }

  @Override
  public void close() {
    cachedBearer = null;
    bearerExpiresAtMillis = 0L;
  }

  private void postJson(long jobId, String action, RESTRequest request) {
    Preconditions.checkArgument(request != null, "request must not be null");
    request.validate();
    String path =
        String.format(
            "/api/metalakes/%s/maintenance/actions/%d/%s",
            RESTUtils.encodeString(metalakeName), jobId, action);
    try {
      HttpResult result =
          post(baseUri + path, JSON, mapper.writeValueAsString(request), authorizationHeader());
      if (!isSuccess(result.status)) {
        throw new IllegalStateException(
            String.format(
                "TMS %s failed: HTTP %d body=%s", action, result.status, truncate(result.body)));
      }
      if (StringUtils.isNotBlank(result.body)) {
        BaseResponse parsed = mapper.readValue(result.body, BaseResponse.class);
        parsed.validate();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to post TMS " + action + ": " + e.getMessage(), e);
    }
  }

  @Nullable
  private String authorizationHeader() {
    switch (authSettings.authType()) {
      case GravitinoAuthSettings.TYPE_SIMPLE:
        return basicAuthorization(simpleUsername(), SIMPLE_AUTH_DUMMY_PASSWORD);
      case GravitinoAuthSettings.TYPE_BASIC:
        requireAuthValue(authSettings.username(), "username is required for basic authentication");
        requireAuthValue(authSettings.password(), "password is required for basic authentication");
        return basicAuthorization(authSettings.username(), authSettings.password());
      case GravitinoAuthSettings.TYPE_OAUTH:
        return AuthConstants.AUTHORIZATION_BEARER_HEADER + oauthAccessToken();
      case GravitinoAuthSettings.TYPE_NONE:
        return null;
      default:
        throw new IllegalArgumentException("Unsupported auth_type: " + authSettings.authType());
    }
  }

  private String simpleUsername() {
    if (StringUtils.isNotBlank(authSettings.username())) {
      return authSettings.username().trim();
    }
    String envUser = System.getenv("GRAVITINO_USER");
    if (StringUtils.isNotBlank(envUser)) {
      return envUser.trim();
    }
    return System.getProperty("user.name");
  }

  private String oauthAccessToken() {
    long now = System.currentTimeMillis();
    if (cachedBearer != null && now < bearerExpiresAtMillis) {
      return cachedBearer;
    }
    requireAuthValue(
        authSettings.oauthServerUri(), "oauth server URI is required for oauth authentication");
    requireAuthValue(authSettings.oauthPath(), "oauth path is required for oauth authentication");
    requireAuthValue(
        authSettings.oauthCredential(), "oauth credential is required for oauth authentication");
    requireAuthValue(authSettings.oauthScope(), "oauth scope is required for oauth authentication");

    String tokenUrl = joinUrl(authSettings.oauthServerUri(), authSettings.oauthPath());
    try {
      HttpResult result = post(tokenUrl, FORM_URLENCODED, buildClientCredentialsForm(), null);
      if (!isSuccess(result.status)) {
        throw new IllegalStateException("OAuth token request failed: HTTP " + result.status);
      }
      OAuth2TokenResponse token = mapper.readValue(result.body, OAuth2TokenResponse.class);
      if (StringUtils.isBlank(token.getAccessToken())) {
        throw new IllegalStateException("OAuth token response did not include access_token");
      }
      cachedBearer = token.getAccessToken();
      int expiresIn =
          token.getExpiresIn() == null ? DEFAULT_OAUTH_EXPIRES_IN_SECONDS : token.getExpiresIn();
      bearerExpiresAtMillis = now + Math.max(1, expiresIn - OAUTH_EXPIRY_SKEW_SECONDS) * 1000L;
      return cachedBearer;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to request OAuth token: " + e.getMessage(), e);
    }
  }

  private String buildClientCredentialsForm() {
    String credential = authSettings.oauthCredential().trim();
    String clientId = null;
    String clientSecret = credential;
    int split = credential.indexOf(':');
    if (split >= 0) {
      clientId = credential.substring(0, split);
      clientSecret = credential.substring(split + 1);
    }
    StringBuilder form = new StringBuilder("grant_type=client_credentials");
    if (StringUtils.isNotBlank(clientId)) {
      form.append("&client_id=").append(urlEncode(clientId));
    }
    form.append("&client_secret=").append(urlEncode(clientSecret));
    form.append("&scope=").append(urlEncode(authSettings.oauthScope().trim()));
    return form.toString();
  }

  private static HttpResult post(
      String url, String contentType, String body, @Nullable String authorization)
      throws IOException {
    HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
    connection.setReadTimeout(READ_TIMEOUT_MS);
    connection.setDoOutput(true);
    connection.setRequestProperty("Accept", ACCEPT);
    connection.setRequestProperty("Content-Type", contentType);
    if (authorization != null) {
      connection.setRequestProperty(AuthConstants.HTTP_HEADER_AUTHORIZATION, authorization);
    }
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    connection.setFixedLengthStreamingMode(bytes.length);
    try {
      try (OutputStream out = connection.getOutputStream()) {
        out.write(bytes);
      }
      int status = connection.getResponseCode();
      InputStream stream =
          isSuccess(status) ? connection.getInputStream() : connection.getErrorStream();
      return new HttpResult(status, readUtf8(stream));
    } finally {
      connection.disconnect();
    }
  }

  @VisibleForTesting
  static Map<String, String> normalizeUpdaterOptions(@Nullable Map<String, String> updaterOptions) {
    Map<String, String> properties =
        updaterOptions == null ? new HashMap<>() : new HashMap<>(updaterOptions);
    putCanonical(properties, OptimizerConfig.GRAVITINO_URI, "gravitino_uri", "gravitino-uri");
    putCanonical(properties, OptimizerConfig.GRAVITINO_METALAKE, "metalake");
    GravitinoAuthSettings.copyAliases(properties);
    return properties;
  }

  private static void putCanonical(
      Map<String, String> properties, String canonical, String... aliases) {
    if (StringUtils.isNotBlank(properties.get(canonical))) {
      return;
    }
    for (String alias : aliases) {
      String value = properties.get(alias);
      if (StringUtils.isNotBlank(value)) {
        properties.put(canonical, value.trim());
        return;
      }
    }
  }

  private static void requireAuthValue(@Nullable String value, String message) {
    Preconditions.checkArgument(StringUtils.isNotBlank(value), message);
  }

  private static boolean isSuccess(int status) {
    return status >= 200 && status < 300;
  }

  private static String basicAuthorization(String user, String password) {
    String userInformation = user + ":" + password;
    return AuthConstants.AUTHORIZATION_BASIC_HEADER
        + Base64.getEncoder().encodeToString(userInformation.getBytes(StandardCharsets.UTF_8));
  }

  private static String joinUrl(String serverUri, String path) {
    String base = trimTrailingSlash(serverUri);
    String suffix = path.startsWith("/") ? path : "/" + path;
    return base + suffix;
  }

  private static String urlEncode(String value) {
    try {
      return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String trimTrailingSlash(String uri) {
    String trimmed = uri.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  private static String readUtf8(@Nullable InputStream stream) throws IOException {
    if (stream == null) {
      return "";
    }
    try (InputStream in = stream;
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[4096];
      int read;
      while ((read = in.read(buffer)) >= 0) {
        out.write(buffer, 0, read);
      }
      return out.toString(StandardCharsets.UTF_8.name());
    }
  }

  private static String truncate(@Nullable String body) {
    if (body == null) {
      return "";
    }
    return body.length() <= ERROR_BODY_TRUNCATE
        ? body
        : body.substring(0, ERROR_BODY_TRUNCATE) + "...";
  }

  private static final class HttpResult {
    private final int status;
    private final String body;

    private HttpResult(int status, String body) {
      this.status = status;
      this.body = body;
    }
  }
}
