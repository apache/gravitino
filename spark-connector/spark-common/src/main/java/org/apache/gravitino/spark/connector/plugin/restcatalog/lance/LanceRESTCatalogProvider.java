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

package org.apache.gravitino.spark.connector.plugin.restcatalog.lance;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.spark.connector.plugin.restcatalog.LakehouseRESTCatalogProvider;
import org.lance.namespace.client.apache.ApiClient;
import org.lance.namespace.client.apache.ApiException;
import org.lance.namespace.client.apache.api.NamespaceApi;
import org.lance.namespace.model.ListNamespacesResponse;

/** Discovers and configures Lance REST catalogs. */
public class LanceRESTCatalogProvider implements LakehouseRESTCatalogProvider {

  static final String FORMAT = "lance";
  static final String CATALOG_CLASS = "org.lance.spark.LanceNamespaceSparkCatalog";
  static final String SPARK_EXTENSIONS = "org.lance.spark.extensions.LanceSparkSessionExtensions";

  private static final String HEADER_PREFIX = "headers.";
  private static final String AUTHORIZATION_PROPERTY = HEADER_PREFIX + "Authorization";
  private static final String ROOT_NAMESPACE_ID = "$";
  private static final String NAMESPACE_DELIMITER = "$";
  static final String NAMESPACE_DELIMITER_PROPERTY = "namespaceDelimiter";

  @Override
  public String format() {
    return FORMAT;
  }

  @Override
  public void normalizeCatalogProperties(Map<String, String> properties) {
    Map<String, String> headers = new LinkedHashMap<>();
    properties.forEach(
        (key, value) -> {
          if (StringUtils.startsWithIgnoreCase(key, HEADER_PREFIX)) {
            String headerName = key.substring(HEADER_PREFIX.length());
            Preconditions.checkArgument(
                headerName.matches("[!#$%&'*+.^_`|~0-9A-Za-z-]+"),
                "Invalid Lance HTTP header name");
            String canonicalKey =
                AUTHORIZATION_PROPERTY.equalsIgnoreCase(key)
                    ? AUTHORIZATION_PROPERTY
                    : HEADER_PREFIX + headerName.toLowerCase(Locale.ROOT);
            String previous = headers.putIfAbsent(canonicalKey, value);
            Preconditions.checkArgument(
                previous == null || previous.equals(value),
                "Conflicting Lance HTTP header values for %s",
                canonicalKey);
          }
        });
    properties.keySet().removeIf(key -> StringUtils.startsWithIgnoreCase(key, HEADER_PREFIX));
    properties.putAll(headers);
  }

  @Override
  public void applyCatalogDefaults(
      Map<String, String> properties, String authType, @Nullable String sparkUser) {
    if (AuthProperties.isSimple(authType)
        && StringUtils.isNotBlank(sparkUser)
        && !properties.containsKey(AUTHORIZATION_PROPERTY)) {
      Preconditions.checkArgument(
          !sparkUser.contains(":"),
          "Spark user for Lance simple authentication must not contain ':'");
      String credentials =
          Base64.getEncoder()
              .encodeToString((sparkUser + ":unused").getBytes(StandardCharsets.UTF_8));
      properties.put(
          AUTHORIZATION_PROPERTY, AuthConstants.AUTHORIZATION_BASIC_HEADER + credentials);
    }
  }

  @Override
  public List<String> listCatalogs(String uri, Map<String, String> catalogProperties) {
    List<String> catalogs = new ArrayList<>();
    Set<String> seenPageTokens = new HashSet<>();
    String namespaceDelimiter =
        catalogProperties.getOrDefault(NAMESPACE_DELIMITER_PROPERTY, NAMESPACE_DELIMITER);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(namespaceDelimiter), "Lance namespace delimiter must not be blank");
    String pageToken = null;

    Map<String, String> requestProperties = new LinkedHashMap<>(catalogProperties);
    normalizeCatalogProperties(requestProperties);

    ApiClient apiClient = new ApiClient().setBasePath(normalizeUri(uri));
    try (Closeable httpClient = getHttpClient(apiClient)) {
      requestProperties.forEach(
          (key, value) -> {
            if (key.startsWith(HEADER_PREFIX)) {
              apiClient.addDefaultHeader(key.substring(HEADER_PREFIX.length()), value);
            }
          });
      NamespaceApi namespaceApi = new NamespaceApi(apiClient);
      do {
        ListNamespacesResponse response =
            namespaceApi.listNamespaces(ROOT_NAMESPACE_ID, namespaceDelimiter, pageToken, null);
        Preconditions.checkState(response != null, "Lance REST server returned an empty response");
        Preconditions.checkState(
            response.getNamespaces() != null,
            "Lance REST server returned a response without namespaces");
        catalogs.addAll(response.getNamespaces());

        pageToken = StringUtils.trimToNull(response.getPageToken());
        Preconditions.checkState(
            pageToken == null || seenPageTokens.add(pageToken),
            "Lance REST server returned repeated page token: %s",
            pageToken);
      } while (pageToken != null);
    } catch (ApiException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to list catalogs from Lance REST server %s: HTTP %d, %s",
              uri, e.getCode(), describeResponse(e)),
          e);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to list catalogs from Lance REST server " + uri, e);
    }

    return catalogs;
  }

  @Override
  public String catalogClassName() {
    return CATALOG_CLASS;
  }

  @Override
  public Map<String, String> generatedCatalogProperties(String uri, String discoveredCatalogName) {
    return ImmutableMap.of("impl", "rest", "uri", uri, "parent", discoveredCatalogName);
  }

  @Override
  public String[] sparkExtensions() {
    return new String[] {SPARK_EXTENSIONS};
  }

  /**
   * The Lance client uses the raw response body as the exception message, which is empty for most
   * error responses, so fall back to the reason phrase and finally to an explicit placeholder
   * instead of reporting a bare {@code ApiException}.
   */
  private static String describeResponse(ApiException e) {
    if (StringUtils.isNotBlank(e.getResponseBody())) {
      return e.getResponseBody();
    }
    if (StringUtils.isNotBlank(e.getMessage())) {
      return e.getMessage();
    }
    return "no response body";
  }

  private static String normalizeUri(String uri) {
    String normalized = uri;
    while (normalized.endsWith("/") && !normalized.endsWith("://")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized;
  }

  private static Closeable getHttpClient(ApiClient apiClient) {
    try {
      // Use reflection to keep Apache HttpClient types out of this class's bytecode. Those types
      // are relocated in Spark runtime JARs but remain unshaded in external Lance bundles, so a
      // direct call can encode an incompatible method descriptor and cause a NoSuchMethodError.
      Object httpClient = ApiClient.class.getMethod("getHttpClient").invoke(apiClient);
      Preconditions.checkState(
          httpClient instanceof Closeable,
          "Lance Namespace ApiClient returned an HTTP client that is not closeable");
      return (Closeable) httpClient;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to access the Lance Namespace HTTP client", e);
    }
  }
}
