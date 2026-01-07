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
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import java.util.List;

/**
 * The GravitinoFTransactionHandle is used to make Apache Gravitino metadata operations
 * transactional and wrap the inner connector transaction for data access.
 */
public class GravitinoSplit implements ConnectorSplit, GravitinoHandle<ConnectorSplit> {

  private HandleWrapper<ConnectorSplit> handleWrapper = new HandleWrapper<>(ConnectorSplit.class);

  /**
   * Constructs a new GravitinoSplit from a serialized handle string.
   *
   * @param handleString the serialized handle string
   */
  @JsonCreator
  public GravitinoSplit(@JsonProperty(HANDLE_STRING) String handleString) {
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  /**
   * Constructs a new GravitinoSplit from a ConnectorSplit.
   *
   * @param split the internal connector split
   */
  public GravitinoSplit(ConnectorSplit split) {
    this.handleWrapper = new HandleWrapper<>(split);
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ConnectorSplit getInternalHandle() {
    return handleWrapper.getHandle();
  }

  @Override
  public boolean isRemotelyAccessible() {
    return handleWrapper.getHandle().isRemotelyAccessible();
  }

  @Override
  public List<HostAddress> getAddresses() {
    return handleWrapper.getHandle().getAddresses();
  }

  @Override
  public Object getInfo() {
    return handleWrapper.getHandle().getInfo();
  }

  @Override
  public SplitWeight getSplitWeight() {
    return handleWrapper.getHandle().getSplitWeight();
  }

  @Override
  public long getRetainedSizeInBytes() {
    return handleWrapper.getHandle().getRetainedSizeInBytes();
  }

  @Override
  public String toString() {
    return DEFAULT_CONNECTOR_NAME + "->" + getInternalHandle().toString();
  }
}
