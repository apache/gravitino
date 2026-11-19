/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.trino.connector;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;
import io.trino.spi.connector.ConnectorTableHandle;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
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
