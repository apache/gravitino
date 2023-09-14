/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public class IcebergRESTConfig extends Config {
  public static final ConfigEntry<Integer> ICEBERG_REST_SERVER_HTTP_PORT =
      new ConfigBuilder("serverPort")
          .doc("The http port number of the Iceberg REST server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(9001);
}
