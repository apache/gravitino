/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;
import io.trino.spi.connector.ConnectorTableHandle;
import org.testng.annotations.Test;

public class TestGravitinoTableHandle {
  private final JsonCodec<GravitinoTableHandle> codec =
      JsonCodec.jsonCodec(GravitinoTableHandle.class);

  @Test
  public void testCreateFromJson() {
    GravitinoTableHandle expected =
        new GravitinoTableHandle("db1", "t1", new MockConnectorTableHandle("mock"));

    String jsonStr = codec.toJson(expected);
    assertTrue(jsonStr.contains("db1"));
    assertTrue(jsonStr.contains("t1"));
    assertTrue(jsonStr.contains("mock"));
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
