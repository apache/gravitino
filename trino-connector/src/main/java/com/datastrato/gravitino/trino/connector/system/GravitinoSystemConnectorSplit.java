/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;
import java.util.Collections;
import java.util.List;

public class GravitinoSystemConnectorSplit implements ConnectorSplit {
  SchemaTableName tableName;

  @JsonCreator
  public GravitinoSystemConnectorSplit(@JsonProperty("tableName") SchemaTableName tableName) {
    this.tableName = tableName;
  }

  @JsonProperty
  public SchemaTableName getTableName() {
    return tableName;
  }

  @Override
  public boolean isRemotelyAccessible() {
    return true;
  }

  @Override
  public List<HostAddress> getAddresses() {
    return List.of(HostAddress.fromParts("127.0.0.1", 8080));
  }

  @Override
  public Object getInfo() {
    return this;
  }
}
