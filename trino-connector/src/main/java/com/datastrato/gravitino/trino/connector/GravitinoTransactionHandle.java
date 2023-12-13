/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 * The GravitinoFTransactionHandle is used to make Gravitino metadata operations transactional and
 * wrap the inner connector transaction for data access.
 */
public class GravitinoTransactionHandle implements ConnectorTransactionHandle {
  ConnectorTransactionHandle internalTransactionHandle;

  @JsonCreator
  public GravitinoTransactionHandle(
      @JsonProperty("internalTransactionHandle")
          ConnectorTransactionHandle internalTransactionHandler) {
    this.internalTransactionHandle = internalTransactionHandler;
  }

  @JsonProperty
  public ConnectorTransactionHandle getInternalTransactionHandle() {
    return internalTransactionHandle;
  }
}
