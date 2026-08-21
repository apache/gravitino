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

package org.apache.gravitino.spark.connector.plugin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.lance.namespace.client.apache.ApiClient;
import org.lance.namespace.client.apache.ApiException;
import org.lance.namespace.client.apache.api.NamespaceApi;
import org.lance.namespace.model.ListNamespacesResponse;

/** Discovers and configures Lance REST catalogs. */
public class LanceRESTCatalogProvider implements LakehouseRESTCatalogProvider {

  static final String FORMAT = "lance";
  static final String CATALOG_CLASS = "org.lance.spark.LanceNamespaceSparkCatalog";
  static final String SPARK_EXTENSIONS = "org.lance.spark.extensions.LanceSparkSessionExtensions";

  private static final String ROOT_NAMESPACE_ID = "$";
  private static final String NAMESPACE_DELIMITER = "$";

  @Override
  public String format() {
    return FORMAT;
  }

  @Override
  public List<String> listCatalogs(String uri, Map<String, String> catalogProperties) {
    List<String> catalogs = new ArrayList<>();
    Set<String> seenPageTokens = new HashSet<>();
    String pageToken = null;

    ApiClient apiClient = new ApiClient().setBasePath(normalizeUri(uri));
    try (Closeable httpClient = getHttpClient(apiClient)) {
      NamespaceApi namespaceApi = new NamespaceApi(apiClient);
      do {
        ListNamespacesResponse response =
            namespaceApi.listNamespaces(ROOT_NAMESPACE_ID, NAMESPACE_DELIMITER, pageToken, null);
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
    } catch (ApiException | IOException e) {
      throw new IllegalStateException("Failed to list catalogs from Lance REST server " + uri, e);
    }

    return catalogs;
  }

  @Override
  public String catalogClassName() {
    return CATALOG_CLASS;
  }

  @Override
  public Map<String, String> generatedCatalogProperties(String uri, String advertisedCatalogName) {
    return ImmutableMap.of("impl", "rest", "uri", uri, "parent", advertisedCatalogName);
  }

  @Override
  public String[] sparkExtensions() {
    return new String[] {SPARK_EXTENSIONS};
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
