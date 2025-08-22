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
import io.trino.spi.connector.ConnectorTableHandle;

/** The GravitinoUpdateTableHandle is used for handling legacy update operations. */
public class GravitinoUpdateTableHandle
    implements ConnectorTableHandle, GravitinoHandle<ConnectorTableHandle> {

  private HandleWrapper<ConnectorTableHandle> handleWrapper =
      new HandleWrapper<>(ConnectorTableHandle.class);

  /**
   * Constructs a new GravitinoUpdateTableHandle from a serialized handle string.
   *
   * @param handleString the serialized handle string
   */
  @JsonCreator
  public GravitinoUpdateTableHandle(@JsonProperty(HANDLE_STRING) String handleString) {
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  /**
   * Constructs a new GravitinoUpdateTableHandle from a ConnectorTableHandle.
   *
   * @param tableHandle the internal connector table handle for update operations
   */
  public GravitinoUpdateTableHandle(ConnectorTableHandle tableHandle) {
    this.handleWrapper = new HandleWrapper<>(tableHandle);
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ConnectorTableHandle getInternalHandle() {
    return handleWrapper.getHandle();
  }
}
