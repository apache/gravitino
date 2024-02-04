/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergConfig {
  @Test
  public void testLoadIcebergConfig() {
    Map<String, String> properties =
        ImmutableMap.of(JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(), "1000");

    IcebergConfig icebergConfig = new IcebergConfig();
    icebergConfig.loadFromMap(properties, k -> k.startsWith("gravitino."));
    Assertions.assertEquals(
        JettyServerConfig.WEBSERVER_HTTP_PORT.getDefaultValue(),
        icebergConfig.get(JettyServerConfig.WEBSERVER_HTTP_PORT));

    IcebergConfig icebergRESTConfig2 = new IcebergConfig(properties);
    Assertions.assertEquals(1000, icebergRESTConfig2.get(JettyServerConfig.WEBSERVER_HTTP_PORT));
  }

  @Test
  public void testIcebergHttpPort() {
    Map<String, String> properties = ImmutableMap.of();
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(icebergConfig);
    Assertions.assertEquals(
        JettyServerConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTP_PORT, jettyServerConfig.getHttpPort());
    Assertions.assertEquals(
        JettyServerConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTPS_PORT,
        jettyServerConfig.getHttpsPort());

    properties =
        ImmutableMap.of(
            JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
            "1000",
            JettyServerConfig.WEBSERVER_HTTPS_PORT.getKey(),
            "1001");
    icebergConfig = new IcebergConfig(properties);
    jettyServerConfig = JettyServerConfig.fromConfig(icebergConfig);
    Assertions.assertEquals(1000, jettyServerConfig.getHttpPort());
    Assertions.assertEquals(1001, jettyServerConfig.getHttpsPort());
  }
}
