/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

public class GravitinoSystemTableHandle implements ConnectorTableHandle {
  SchemaTableName name;

  @JsonCreator
  public GravitinoSystemTableHandle(@JsonProperty("name") SchemaTableName name) {
    this.name = name;
  }

  @JsonProperty
  public SchemaTableName getName() {
    return name;
  }
}
