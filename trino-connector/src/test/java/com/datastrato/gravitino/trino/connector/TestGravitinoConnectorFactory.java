/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.datastrato.gravitino.client.GravitinoClient;
import java.util.function.Supplier;

public class TestGravitinoConnectorFactory extends GravitinoConnectorFactory {
  private GravitinoClient gravitinoClient;

  public void setGravitinoClient(GravitinoClient client) {
    this.gravitinoClient = client;
  }

  @Override
  Supplier<GravitinoClient> clientProvider() {
    return () -> gravitinoClient;
  }
}
