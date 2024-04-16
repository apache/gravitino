/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

/** Trino plugin endpoint, using java spi mechanism */
public class GravitinoPlugin implements Plugin {

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    return ImmutableList.of(new GravitinoConnectorFactory());
  }
}
