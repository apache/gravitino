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
package org.apache.gravitino.iceberg.service.rest;

import com.google.common.base.Joiner;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.iceberg.service.IcebergObjectMapperProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.jupiter.api.Assertions;

public class IcebergTestBase extends JerseyTest {
  @Override
  protected Application configure() {
    return new ResourceConfig();
  }

  private String urlPathPrefix = "";

  public Invocation.Builder getRenameTableClientBuilder() {
    return getIcebergClientBuilder(IcebergRestTestUtil.RENAME_TABLE_PATH, Optional.empty());
  }

  public Invocation.Builder getRenameViewClientBuilder() {
    return getIcebergClientBuilder(IcebergRestTestUtil.RENAME_VIEW_PATH, Optional.empty());
  }

  public Invocation.Builder getTableClientBuilder() {
    return getTableClientBuilder(Optional.empty());
  }

  public Invocation.Builder getViewClientBuilder() {
    return getViewClientBuilder(Optional.empty());
  }

  public Invocation.Builder getTableClientBuilder(Optional<String> name) {
    String path =
        Joiner.on("/").skipNulls().join(IcebergRestTestUtil.TABLE_PATH, name.orElseGet(() -> null));
    return getIcebergClientBuilder(path, Optional.empty());
  }

  public Invocation.Builder getViewClientBuilder(Optional<String> name) {
    String path =
        Joiner.on("/").skipNulls().join(IcebergRestTestUtil.VIEW_PATH, name.orElseGet(() -> null));
    return getIcebergClientBuilder(path, Optional.empty());
  }

  public Invocation.Builder getReportMetricsClientBuilder(String name) {
    String path =
        Joiner.on("/")
            .skipNulls()
            .join(IcebergRestTestUtil.TABLE_PATH, name, IcebergRestTestUtil.REPORT_METRICS_POSTFIX);
    return getIcebergClientBuilder(path, Optional.empty());
  }

  public Invocation.Builder getNamespaceClientBuilder() {
    return getNamespaceClientBuilder(Optional.empty(), Optional.empty(), Optional.empty());
  }

  public Invocation.Builder getNamespaceClientBuilder(Optional<String> namespace) {
    return getNamespaceClientBuilder(namespace, Optional.empty(), Optional.empty());
  }

  public Invocation.Builder getNamespaceClientBuilder(
      Optional<String> namespace,
      Optional<String> extraPath,
      Optional<Map<String, String>> queryParams) {
    String path =
        Joiner.on("/")
            .skipNulls()
            .join(
                IcebergRestTestUtil.NAMESPACE_PATH,
                namespace.orElseGet(() -> null),
                extraPath.orElseGet(() -> null));
    return getIcebergClientBuilder(path, queryParams);
  }

  public Invocation.Builder getUpdateNamespaceClientBuilder(String namespace) {
    String path =
        Joiner.on("/")
            .skipNulls()
            .join(
                IcebergRestTestUtil.NAMESPACE_PATH,
                namespace,
                IcebergRestTestUtil.UPDATE_NAMESPACE_POSTFIX);
    return getIcebergClientBuilder(path, Optional.empty());
  }

  public Invocation.Builder getConfigClientBuilder() {
    return getIcebergClientBuilder(IcebergRestTestUtil.CONFIG_PATH, Optional.empty());
  }

  public String injectPrefixToPath(String path, String prefix) {
    Joiner joiner = Joiner.on("/");
    String[] items = path.split("/");
    Assertions.assertTrue(items.length > 0);
    String[] newItems = ArrayUtils.insert(1, items, prefix);
    return joiner.join(newItems);
  }

  public Invocation.Builder getIcebergClientBuilder(
      String path, Optional<Map<String, String>> queryParam) {
    if (!StringUtils.isBlank(urlPathPrefix)) {
      path = injectPrefixToPath(path, urlPathPrefix);
    }
    WebTarget target = target(path);
    if (queryParam.isPresent()) {
      Map<String, String> m = queryParam.get();
      for (Entry<String, String> entry : m.entrySet()) {
        target = target.queryParam(entry.getKey(), entry.getValue());
      }
    }

    return target
        .register(IcebergObjectMapperProvider.class)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE);
  }

  public void setUrlPathWithPrefix(String urlPathPrefix) {
    this.urlPathPrefix = urlPathPrefix;
  }
}
