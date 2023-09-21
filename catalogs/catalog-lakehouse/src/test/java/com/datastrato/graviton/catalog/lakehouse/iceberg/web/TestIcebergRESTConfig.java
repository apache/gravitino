/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg.web;

import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergRESTConfig;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergRESTConfig {
  @Test
  public void testIcebergConfig() {
    Map properties =
        ImmutableMap.of(IcebergRESTConfig.ICEBERG_REST_SERVER_HTTP_PORT.getKey(), "1000");

    IcebergRESTConfig icebergRESTConfig = new IcebergRESTConfig();
    icebergRESTConfig.loadFromMap(properties, k -> k.startsWith("graviton."));
    Assertions.assertEquals(
        IcebergRESTConfig.ICEBERG_REST_SERVER_HTTP_PORT.getDefaultValue(),
        icebergRESTConfig.get(IcebergRESTConfig.ICEBERG_REST_SERVER_HTTP_PORT));

    IcebergRESTConfig icebergRESTConfig2 = new IcebergRESTConfig();
    icebergRESTConfig2.loadFromMap(properties, k -> true);
    Assertions.assertEquals(
        1000, icebergRESTConfig2.get(IcebergRESTConfig.ICEBERG_REST_SERVER_HTTP_PORT));
  }
}
