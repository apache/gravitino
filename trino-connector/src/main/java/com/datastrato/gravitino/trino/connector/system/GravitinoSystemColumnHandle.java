/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;

public class GravitinoSystemColumnHandle implements ColumnHandle {
  int index;

  @JsonCreator
  public GravitinoSystemColumnHandle(@JsonProperty("index") int index) {
    this.index = index;
  }

  @JsonProperty
  public int getIndex() {
    return index;
  }
}
