/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;

/** The GravitonInsertTableHandle is used for handling insert operations. */
public class GravitonInsertTableHandle implements ConnectorInsertTableHandle {

  private final ConnectorInsertTableHandle internalInsertTableHandle;

  @JsonCreator
  public GravitonInsertTableHandle(
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
