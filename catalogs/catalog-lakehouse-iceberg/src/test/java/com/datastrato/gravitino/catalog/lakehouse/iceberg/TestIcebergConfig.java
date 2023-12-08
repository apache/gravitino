/*
 * Copyright 2023 Datastrato.
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
  public void testIcebergConfig() {
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
}
