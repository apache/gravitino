/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;

/** The GravitinoInsertTableHandle is used for handling insert operations. */
public class GravitinoInsertTableHandle implements ConnectorInsertTableHandle {

  private final ConnectorInsertTableHandle internalInsertTableHandle;

  @JsonCreator
  public GravitinoInsertTableHandle(
      @JsonProperty("internalInsertTableHandle")
          ConnectorInsertTableHandle internalInsertTableHandle) {
    this.internalInsertTableHandle = internalInsertTableHandle;
  }

  @JsonProperty
  public ConnectorInsertTableHandle getInternalInsertTableHandle() {
    return internalInsertTableHandle;
  }

  public ConnectorInsertTableHandle innerHandler() {
    return internalInsertTableHandle;
  }
}
