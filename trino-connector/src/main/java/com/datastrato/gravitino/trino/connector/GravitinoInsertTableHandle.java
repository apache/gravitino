/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;

/** The GravitinoInsertTableHandle is used for handling insert operations. */
public class GravitinoInsertTableHandle
    implements ConnectorInsertTableHandle, GravitinoHandle<ConnectorInsertTableHandle> {

  private HandleWrapper<ConnectorInsertTableHandle> handleWrapper =
      new HandleWrapper<>(ConnectorInsertTableHandle.class);

  @JsonCreator
  public GravitinoInsertTableHandle(@JsonProperty(HANDLE_STRING) String handleString) {
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  public GravitinoInsertTableHandle(ConnectorInsertTableHandle insertTableHandle) {
    this.handleWrapper = new HandleWrapper<>(insertTableHandle);
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ConnectorInsertTableHandle getInternalHandle() {
    return handleWrapper.getHandle();
  }
}
