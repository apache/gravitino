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

  public static final ConfigOption<String> METALAKE_URI =
      ConfigOptions.key("metalake.uri")
          .stringType()
          .noDefaultValue()
          .withDescription("The uri of gravitino metalake");
  public static final ConfigOption<String> METALAKE_NAME =
      ConfigOptions.key("metalake.name")
          .stringType()
          .noDefaultValue()
          .withDescription("The name of gravitino metalake");
}
