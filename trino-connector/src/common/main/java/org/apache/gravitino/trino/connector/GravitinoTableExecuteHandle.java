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
import io.trino.spi.connector.ConnectorTableExecuteHandle;

/** The GravitinoTableExecuteHandle is used for handling table execute operations. */
public class GravitinoTableExecuteHandle
    implements ConnectorTableExecuteHandle, GravitinoHandle<ConnectorTableExecuteHandle> {

  private HandleWrapper<ConnectorTableExecuteHandle> handleWrapper =
      new HandleWrapper<>(ConnectorTableExecuteHandle.class);

  /**
   * Constructs a new GravitinoTableExecuteHandle from a serialized handle string.
   *
   * @param handleString the serialized handle string
   */
  @JsonCreator
  public GravitinoTableExecuteHandle(@JsonProperty(HANDLE_STRING) String handleString) {
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  /**
   * Constructs a new GravitinoTableExecuteHandle from a ConnectorTableExecuteHandle.
   *
   * @param tableExecuteHandle the internal connector table execute handle
   */
  public GravitinoTableExecuteHandle(ConnectorTableExecuteHandle tableExecuteHandle) {
    this.handleWrapper = new HandleWrapper<>(tableExecuteHandle);
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ConnectorTableExecuteHandle getInternalHandle() {
    return handleWrapper.getHandle();
  }
}
