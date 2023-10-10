/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;
import io.trino.spi.connector.ConnectorTableHandle;
import org.junit.jupiter.api.Test;

public class TestGravitonTableHandle {
  private final JsonCodec<GravitonTableHandle> codec =
      JsonCodec.jsonCodec(GravitonTableHandle.class);

  @Test
  public void testCreateFromJson() {
    GravitonTableHandle expected =
        new GravitonTableHandle("db1", "t1", new MockConnectorTableHandle("mock"));

    codec.toJson(expected);
  }

  public static class MockConnectorTableHandle implements ConnectorTableHandle {

    private final String name;

    @JsonCreator
    public MockConnectorTableHandle(@JsonProperty("name") String name) {
      this.name = name;
    }

    @JsonProperty
    public String getName() {
      return name;
    }
  }
}
