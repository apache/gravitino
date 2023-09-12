/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg.web.rest;

import com.datastrato.graviton.catalog.lakehouse.iceberg.web.IcebergObjectMapperProvider;
import com.datastrato.graviton.rest.RESTUtils;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.glassfish.jersey.test.JerseyTest;

public class IcebergTestBase extends JerseyTest {
  static {
    int port;
    try {
      port = RESTUtils.findAvailablePort("2000:3000");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    System.setProperty("jersey.config.test.container.port", String.valueOf(port));
  }

  public Builder getRenameTableClientBuilder() {
    return getIcebergClientBuilder(IcebergRestTestUtil.RENAME_TABLE_PATH, Optional.empty());
  }

  public Builder getTableClientBuilder() {
    return getTableClientBuilder(Optional.empty());
  }

  public Builder getTableClientBuilder(Optional<String> name) {
    String path =
        Joiner.on("/").skipNulls().join(IcebergRestTestUtil.TABLE_PATH, name.orElseGet(() -> null));
    return getIcebergClientBuilder(path, Optional.empty());
  }

  public Builder getNamespaceClientBuilder() {
    return getNamespaceClientBuilder(Optional.empty(), Optional.empty());
  }

  public Builder getNamespaceClientBuilder(Optional<String> namespace) {
    return getNamespaceClientBuilder(namespace, Optional.empty());
  }

  public Builder getNamespaceClientBuilder(
      Optional<String> namespace, Optional<Map<String, String>> queryParams) {
    String path =
        Joiner.on("/")
            .skipNulls()
            .join(IcebergRestTestUtil.NAMESPACE_PATH, namespace.orElseGet(() -> null));
    return getIcebergClientBuilder(path, queryParams);
  }

  public Builder getConfigClientBuilder() {
    return getIcebergClientBuilder(IcebergRestTestUtil.CONFIG_PATH, Optional.empty());
  }

  private Builder getIcebergClientBuilder(String path, Optional<Map<String, String>> queryParam) {
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
}
