/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 * The GravitonTransactionHandle is used to make Graviton metadata operations transactional and wrap
 * the inner connector transaction for data access.
 */
public class GravitonTransactionHandle implements ConnectorTransactionHandle {
  ConnectorTransactionHandle internalTransactionHandle;

  @JsonCreator
  public GravitonTransactionHandle(
      @JsonProperty("internalTransactionHandle")
          ConnectorTransactionHandle internalTransactionHandler) {
    this.internalTransactionHandle = internalTransactionHandler;
  }

  @JsonProperty
  public ConnectorTransactionHandle getInternalTransactionHandle() {
    return internalTransactionHandle;
  }
}
