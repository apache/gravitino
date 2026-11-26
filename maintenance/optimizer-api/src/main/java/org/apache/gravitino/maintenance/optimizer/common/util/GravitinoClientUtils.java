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

package org.apache.gravitino.maintenance.optimizer.common.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClientConfiguration;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/** Utility methods for creating Gravitino clients from optimizer configuration. */
public final class GravitinoClientUtils {

  private static final String SCHEMA_SEPARATOR_CONFIG_KEY = "gravitino.schema.separator";

  private GravitinoClientUtils() {}

  /**
   * Loads {@code gravitino.schema.separator} from the unauthenticated {@code GET /configs}
   * endpoint. Does not invent a default when the URI is missing or the key is absent.
   *
   * @param gravitinoUri Gravitino server base URI (required)
   * @return configured external hierarchical schema separator
   */
  public static String fetchSchemaSeparator(String gravitinoUri) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(gravitinoUri),
        "gravitino_uri is required to load gravitino.schema.separator from GET /configs");
    String base = gravitinoUri.trim();
    while (base.endsWith("/")) {
      base = base.substring(0, base.length() - 1);
    }
    String url = base + "/configs";
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
      connection.setRequestMethod("GET");
      connection.setConnectTimeout(30_000);
      connection.setReadTimeout(30_000);
      connection.setRequestProperty("Accept", "application/json");
      int status = connection.getResponseCode();
      InputStream stream =
          status >= 200 && status < 300 ? connection.getInputStream() : connection.getErrorStream();
      String body = readUtf8(stream);
      if (status < 200 || status >= 300) {
        throw new IllegalStateException(
            "GET "
                + url
                + " returned HTTP "
                + status
                + (StringUtils.isBlank(body) ? "" : ": " + body));
      }
      Map<String, Object> configs =
          JsonUtils.objectMapper().readValue(body, new TypeReference<Map<String, Object>>() {});
      Object value = configs.get(SCHEMA_SEPARATOR_CONFIG_KEY);
      Preconditions.checkArgument(
          value != null && StringUtils.isNotBlank(String.valueOf(value)),
          "GET %s did not return %s",
          url,
          SCHEMA_SEPARATOR_CONFIG_KEY);
      return String.valueOf(value).trim();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load gravitino.schema.separator from " + url, e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static String readUtf8(@Nullable InputStream stream) throws IOException {
    if (stream == null) {
      return "";
    }
    try (InputStream in = stream) {
      byte[] buffer = new byte[4096];
      StringBuilder out = new StringBuilder();
      int read;
      while ((read = in.read(buffer)) >= 0) {
        out.append(new String(buffer, 0, read, StandardCharsets.UTF_8));
      }
      return out.toString();
    }
  }

  /**
   * Promotes short updater-option aliases onto canonical {@link OptimizerConfig} keys and copies
   * auth aliases. Does not invent defaults or validate required fields.
   *
   * @param options raw {@code updater-options} map
   * @return a new map with connection aliases normalized
   */
  public static Map<String, String> normalizeUpdaterConnectionOptions(Map<String, String> options) {
    Preconditions.checkArgument(options != null, "options must not be null");
    Map<String, String> properties = new HashMap<>(options);
    firstNonEmpty(
            options.get("gravitino_uri"),
            options.get("gravitino-uri"),
            options.get(OptimizerConfig.GRAVITINO_URI))
        .ifPresent(uri -> properties.put(OptimizerConfig.GRAVITINO_URI, uri));
    firstNonEmpty(options.get("metalake"), options.get(OptimizerConfig.GRAVITINO_METALAKE))
        .ifPresent(value -> properties.put(OptimizerConfig.GRAVITINO_METALAKE, value));
    GravitinoAuthSettings.copyAliases(properties);
    return properties;
  }

  /**
   * Requires a non-blank {@link OptimizerConfig#GRAVITINO_URI} after alias normalization, so jobs
   * can load {@code gravitino.schema.separator} from {@code GET /configs}.
   *
   * @param optimizerProperties normalized updater options
   */
  public static void requireGravitinoUri(Map<String, String> optimizerProperties) {
    Preconditions.checkArgument(
        optimizerProperties != null, "optimizerProperties must not be null");
    String gravitinoUri = optimizerProperties.get(OptimizerConfig.GRAVITINO_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(gravitinoUri), "updater_options must contain 'gravitino_uri'");
  }

  /**
   * When {@code updateMode} is {@code metrics} or {@code all}, requires non-blank JDBC metrics
   * settings in {@code updaterOptions}: {@link OptimizerConfig#JDBC_METRICS_JDBC_URL}, {@link
   * OptimizerConfig#JDBC_METRICS_JDBC_USER}, and {@link OptimizerConfig#JDBC_METRICS_JDBC_DRIVER}.
   * {@link OptimizerConfig#JDBC_METRICS_JDBC_PASSWORD} may be blank. {@code stats} skips this
   * check. Shared by the built-in update-stats Spark job and {@code submit-update-stats-job}.
   *
   * @param updateMode update-stats mode ({@code stats}, {@code metrics}, or {@code all})
   * @param updaterOptions updater-options map (raw or normalized)
   */
  public static void requireJdbcMetricsConfigIfNeeded(
      String updateMode, Map<String, String> updaterOptions) {
    Preconditions.checkArgument(updaterOptions != null, "updaterOptions must not be null");
    String mode = updateMode == null ? "" : updateMode.trim().toLowerCase(Locale.ROOT);
    if (!"metrics".equals(mode) && !"all".equals(mode)) {
      return;
    }
    requireNonBlankJdbcMetricsProperty(updaterOptions, OptimizerConfig.JDBC_METRICS_JDBC_URL);
    requireNonBlankJdbcMetricsProperty(updaterOptions, OptimizerConfig.JDBC_METRICS_JDBC_USER);
    requireNonBlankJdbcMetricsProperty(updaterOptions, OptimizerConfig.JDBC_METRICS_JDBC_DRIVER);
  }

  private static void requireNonBlankJdbcMetricsProperty(
      Map<String, String> updaterOptions, String propertyKey) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(updaterOptions.get(propertyKey)),
        "updater_options must contain '" + propertyKey + "' when update_mode is metrics or all");
  }

  private static Optional<String> firstNonEmpty(String... candidates) {
    for (String candidate : candidates) {
      if (StringUtils.isNotBlank(candidate)) {
        return Optional.of(candidate.trim());
      }
    }
    return Optional.empty();
  }

  /**
   * Creates a {@link GravitinoClient} using optimizer configuration.
   *
   * <p>When the config carries auth keys (for example from update-stats {@code updater-options}
   * loaded into {@link OptimizerConfig}), those credentials are applied to the client builder.
   *
   * @param optimizerEnv optimizer environment
   * @return configured Gravitino client
   */
  public static GravitinoClient createClient(OptimizerEnv optimizerEnv) {
    return createClient(optimizerEnv, null);
  }

  /**
   * Creates a {@link GravitinoClient} using URI/metalake from {@code optimizerEnv} and optional
   * auth from {@code authFromUpdaterOptions} (short names such as {@code auth_type}).
   *
   * <p>Used by {@code submit-update-stats-job} so the CLI can call {@code runJob} against a secured
   * server with the same credentials that the job will use.
   *
   * @param optimizerEnv optimizer environment (URI and metalake)
   * @param authFromUpdaterOptions updater-options map, may be {@code null}
   * @return configured Gravitino client
   */
  public static GravitinoClient createClient(
      OptimizerEnv optimizerEnv, @Nullable Map<String, String> authFromUpdaterOptions) {
    Preconditions.checkArgument(optimizerEnv != null, "optimizerEnv must not be null");
    OptimizerConfig config = optimizerEnv.config();
    String uri = config.get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    GravitinoClient.ClientBuilder builder = GravitinoClient.builder(uri).withMetalake(metalake);

    String retryTimes = config.getRawString(GravitinoClientConfiguration.CLIENT_RETRY_TIMES);
    if (retryTimes != null) {
      builder.withClientConfig(
          Collections.singletonMap(GravitinoClientConfiguration.CLIENT_RETRY_TIMES, retryTimes));
    }

    OptimizerConfig authConfig = config;
    if (authFromUpdaterOptions != null && !authFromUpdaterOptions.isEmpty()) {
      Map<String, String> authProperties = new HashMap<>(authFromUpdaterOptions);
      GravitinoAuthSettings.copyAliases(authProperties);
      authConfig = new OptimizerConfig(authProperties);
    }
    GravitinoAuthSettings.from(authConfig).applyTo(builder);
    return builder.build();
  }
}
