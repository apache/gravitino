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

import com.google.common.base.Preconditions;
import org.apache.gravitino.annotation.DeveloperApi;

/** JSON payload for an Iceberg REST response used by listener events. */
@DeveloperApi
public class IcebergResponsePayload {

  private final String type;
  private final String json;

  /**
   * Constructs a new {@code IcebergResponsePayload}.
   *
   * @param type The simple response type name.
   * @param json The serialized response JSON.
   */
  public IcebergResponsePayload(String type, String json) {
    Preconditions.checkArgument(type != null && !type.isBlank(), "type cannot be blank");
    Preconditions.checkArgument(json != null && !json.isBlank(), "json cannot be blank");
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
}
