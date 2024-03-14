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
