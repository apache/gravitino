/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.server.web.JettyServerContext;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergConfig {
  @Test
  public void testIcebergConfig() {
    Map<String, String> properties =
        ImmutableMap.of(JettyServerContext.WEBSERVER_HTTP_PORT.getKey(), "1000");

    IcebergConfig icebergConfig = new IcebergConfig();
    icebergConfig.loadFromMap(properties, k -> k.startsWith("graviton."));
    Assertions.assertEquals(
        JettyServerContext.WEBSERVER_HTTP_PORT.getDefaultValue(),
        icebergConfig.get(JettyServerContext.WEBSERVER_HTTP_PORT));

    IcebergConfig icebergRESTConfig2 = new IcebergConfig();
    icebergRESTConfig2.loadFromMap(properties, k -> true);
    Assertions.assertEquals(1000, icebergRESTConfig2.get(JettyServerContext.WEBSERVER_HTTP_PORT));
  }
}
