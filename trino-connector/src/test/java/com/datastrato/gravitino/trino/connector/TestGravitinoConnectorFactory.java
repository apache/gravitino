/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import java.util.Map;

class TestGravitinoConnectorFactory extends GravitinoConnectorFactory {
  private Runnable hook;

  void setHook(Runnable hook) {
    this.hook = hook;
  }

  @Override
  public Connector create(
      String catalogName, Map<String, String> requiredConfig, ConnectorContext context) {
    Connector connector = super.create(catalogName, requiredConfig, context);
    hook.run();
    return connector;
  }
}
