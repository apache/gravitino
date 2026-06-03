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

/** JSON payload for an Iceberg REST request used by listener events. */
public class IcebergRequestPayload {

  private final String type;
  private final String json;

  /**
   * Constructs a new {@code IcebergRequestPayload}.
   *
   * @param type The simple request type name.
   * @param json The serialized request JSON.
   */
  public IcebergRequestPayload(String type, String json) {
    requireNonBlank(type, "type");
    requireNonBlank(json, "json");
    this.type = type;
    this.json = json;
  }

  /**
   * Returns the payload type name.
   *
   * @return The payload type name.
   */
  public String type() {
    return type;
  }

  /**
   * Returns the serialized JSON payload.
   *
   * @return The serialized JSON payload.
   */
  public String json() {
    return json;
  }

  private static void requireNonBlank(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " cannot be blank");
    }
  }
}
