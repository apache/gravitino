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
package org.apache.gravitino.client;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoClientConfiguration {
  @Test
  void testValidConfig() {
    Map<String, String> properties =
        ImmutableMap.of(
            "gravitino.client.connectionTimeoutMs", String.valueOf(20),
            "gravitino.client.socketTimeoutMs", String.valueOf(10));
    GravitinoClientConfiguration clientConfiguration =
        GravitinoClientConfiguration.buildFromProperties(properties);
    Assertions.assertEquals(clientConfiguration.getClientConnectionTimeoutMs(), 20L);
    Assertions.assertEquals(clientConfiguration.getClientSocketTimeoutMs(), 10);
  }

  @Test
  void testInValidConfig() {

    Map<String, String> properties =
        ImmutableMap.of(
            "gravitino.client.xxxx", String.valueOf(10),
            "gravitino.client.socketTimeoutMs", String.valueOf(20));
    Throwable throwable =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoClientConfiguration.buildFromProperties(properties));
    Assertions.assertEquals(
        "Invalid property for client: gravitino.client.xxxx", throwable.getMessage());
  }
}
