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

import static org.apache.gravitino.trino.connector.GravitinoConnectorFactory.DEFAULT_CONNECTOR_NAME;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 * The GravitinoFTransactionHandle is used to make Apache Gravitino metadata operations
 * transactional and wrap the inner connector transaction for data access.
 */
public class GravitinoTransactionHandle
    implements ConnectorTransactionHandle, GravitinoHandle<ConnectorTransactionHandle> {

  private HandleWrapper<ConnectorTransactionHandle> handleWrapper =
      new HandleWrapper<>(ConnectorTransactionHandle.class);

  /**
   * Constructs a new GravitinoTransactionHandle from a handle string.
   *
   * @param handleString the handle string representation
   */
  @JsonCreator
  public GravitinoTransactionHandle(@JsonProperty(HANDLE_STRING) String handleString) {
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  /**
   * Constructs a new GravitinoTransactionHandle from an internal transaction handle.
   *
   * @param internalTransactionHandle the internal transaction handle
   */
  public GravitinoTransactionHandle(ConnectorTransactionHandle internalTransactionHandle) {
    this.handleWrapper = new HandleWrapper<>(internalTransactionHandle);
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ConnectorTransactionHandle getInternalHandle() {
    return handleWrapper.getHandle();
  }

  @Override
  public String toString() {
    return DEFAULT_CONNECTOR_NAME + "->" + getInternalHandle().toString();
  }
}
