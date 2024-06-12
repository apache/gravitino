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
public class GravitinoTransactionHandle
    implements ConnectorTransactionHandle, GravitinoHandle<ConnectorTransactionHandle> {

  private HandleWrapper<ConnectorTransactionHandle> handleWrapper =
      new HandleWrapper<>(ConnectorTransactionHandle.class);

  @JsonCreator
  public GravitinoTransactionHandle(@JsonProperty(HANDLE_STRING) String handleString) {
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

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
}
