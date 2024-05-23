/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.store;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class GravitinoCatalogStoreFactoryOptions {

  private GravitinoCatalogStoreFactoryOptions() {}

  public static final String GRAVITINO = "gravitino";

  public static final ConfigOption<String> GRAVITINO_URI =
      ConfigOptions.key("gravitino.uri")
          .stringType()
          .noDefaultValue()
          .withDescription("The uri of Gravitino server");
  public static final ConfigOption<String> GRAVITINO_METALAKE =
      ConfigOptions.key("gravitino.metalake")
          .stringType()
          .noDefaultValue()
          .withDescription("The name of Gravitino metalake");
}
