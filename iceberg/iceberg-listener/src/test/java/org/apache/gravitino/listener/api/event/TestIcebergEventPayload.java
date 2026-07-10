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

package org.apache.gravitino.listener.api.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Tests for Iceberg listener payload value objects. */
public class TestIcebergEventPayload {

  @Test
  public void testRequestPayload() {
    IcebergRequestPayload payload =
        new IcebergRequestPayload("CreateTableRequest", "{\"name\":\"t\"}");

    assertEquals("CreateTableRequest", payload.type());
    assertEquals("{\"name\":\"t\"}", payload.json());
  }

  @Test
  public void testResponsePayload() {
    IcebergResponsePayload payload =
        new IcebergResponsePayload("LoadTableResponse", "{\"metadata-location\":\"s3://t\"}");

    assertEquals("LoadTableResponse", payload.type());
    assertEquals("{\"metadata-location\":\"s3://t\"}", payload.json());
  }

  @Test
  public void testRejectsBlankValues() {
    assertThrows(IllegalArgumentException.class, () -> new IcebergRequestPayload("", "{}"));
    assertThrows(IllegalArgumentException.class, () -> new IcebergRequestPayload("type", ""));
    assertThrows(IllegalArgumentException.class, () -> new IcebergResponsePayload("", "{}"));
    assertThrows(IllegalArgumentException.class, () -> new IcebergResponsePayload("type", ""));
  }
}
