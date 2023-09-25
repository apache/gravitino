/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

public class GravitonPlugin implements Plugin {
  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    return ImmutableList.of(new GravitonConnectorFactory());
  }
}
