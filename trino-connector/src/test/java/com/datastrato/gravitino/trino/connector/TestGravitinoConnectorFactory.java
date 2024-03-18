/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.datastrato.gravitino.client.GravitinoAdminClient;
import java.util.function.Supplier;

public class TestGravitinoConnectorFactory extends GravitinoConnectorFactory {
  private GravitinoAdminClient gravitinoClient;

  public void setGravitinoClient(GravitinoAdminClient client) {
    this.gravitinoClient = client;
  }

  @Override
  Supplier<GravitinoAdminClient> clientProvider() {
    return () -> gravitinoClient;
  }
}
