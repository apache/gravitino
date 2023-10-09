/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public class ServerConfig extends Config {

  public static final ConfigEntry<Integer> SERVER_SHUTDOWN_TIMEOUT =
      new ConfigBuilder("graviton.server.shutdown.timeout")
          .doc("The stop idle timeout(millis) of the Graviton Server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(3 * 1000);

  public ServerConfig(boolean loadDefaults) {
    super(loadDefaults);
  }

  public ServerConfig() {
    this(true);
  }
}
