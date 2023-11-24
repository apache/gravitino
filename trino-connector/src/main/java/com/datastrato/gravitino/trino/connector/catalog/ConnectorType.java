/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog;

public enum ConnectorType {
  MEMORY("memory"),
  HIVE("hive"),
  ICEBERG("lakehouse-iceberg");

  private final String name;

  ConnectorType(String name) {
    this.name = name;
  }

  public static ConnectorType fromName(String name) {
    for (ConnectorType connectorType : ConnectorType.values()) {
      if (connectorType.name.equals(name)) {
        return connectorType;
      }
    }
    throw new IllegalArgumentException("Unknown connector type: " + name);
  }
}
