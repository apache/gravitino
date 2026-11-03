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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableCredentials;

/**
 * Wraps an internal connector's {@link ConnectorTableCredentials}. Trino resolves this credentials
 * type by the concrete class returned from the connector, so the real (e.g. Iceberg) credentials
 * object must be wrapped under this Gravitino-registered type rather than returned as-is, the same
 * way {@link GravitinoSplit} wraps {@code ConnectorSplit}.
 */
public class GravitinoTableCredentials
    implements ConnectorTableCredentials, GravitinoHandle<ConnectorTableCredentials> {

  private HandleWrapper<ConnectorTableCredentials> handleWrapper =
      new HandleWrapper<>(ConnectorTableCredentials.class);

  /**
   * Constructs a new GravitinoTableCredentials from a serialized handle string.
   *
   * @param handleString the serialized handle string
   */
  @JsonCreator
  public GravitinoTableCredentials(@JsonProperty(HANDLE_STRING) String handleString) {
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  /**
   * Constructs a new GravitinoTableCredentials from a ConnectorTableCredentials.
   *
   * @param credentials the internal connector table credentials
   */
  public GravitinoTableCredentials(ConnectorTableCredentials credentials) {
    this.handleWrapper = new HandleWrapper<>(credentials);
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ConnectorTableCredentials getInternalHandle() {
    return handleWrapper.getHandle();
  }
}
