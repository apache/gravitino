/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.rest;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergObjectMapperProvider;
import com.datastrato.gravitino.rest.RESTUtils;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.ArrayUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;

public class IcebergTestBase extends JerseyTest {
  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new ResourceConfig();
  }

  private boolean urlPathWithPrefix = false;

  public Invocation.Builder getRenameTableClientBuilder() {
    return getIcebergClientBuilder(IcebergRestTestUtil.RENAME_TABLE_PATH, Optional.empty());
  }

  public Invocation.Builder getTableClientBuilder() {
    return getTableClientBuilder(Optional.empty());
  }

  public Invocation.Builder getTableClientBuilder(Optional<String> name) {
    String path =
        Joiner.on("/").skipNulls().join(IcebergRestTestUtil.TABLE_PATH, name.orElseGet(() -> null));
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
    return getNamespaceClientBuilder(Optional.empty(), Optional.empty());
  }

  public Invocation.Builder getNamespaceClientBuilder(Optional<String> namespace) {
    return getNamespaceClientBuilder(namespace, Optional.empty());
  }

  public Invocation.Builder getNamespaceClientBuilder(
      Optional<String> namespace, Optional<Map<String, String>> queryParams) {
    String path =
        Joiner.on("/")
            .skipNulls()
            .join(IcebergRestTestUtil.NAMESPACE_PATH, namespace.orElseGet(() -> null));
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
    if (urlPathWithPrefix) {
      path = injectPrefixToPath(path, IcebergRestTestUtil.PREFIX);
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

  public void setUrlPathWithPrefix(boolean urlPathWithPrefix) {
    this.urlPathWithPrefix = urlPathWithPrefix;
  }
}
