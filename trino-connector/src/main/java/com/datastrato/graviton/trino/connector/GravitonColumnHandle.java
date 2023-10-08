/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;

public final class GravitonColumnHandle implements ColumnHandle {
  private String columnName;
  private ColumnHandle internalColumnHandler;

  @JsonCreator
  public GravitonColumnHandle(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("internalColumnHandler") ColumnHandle columnHandle) {
    this.columnName = requireNonNull(columnName, "column is null");
    this.internalColumnHandler = requireNonNull(columnHandle, "column is null");
  }

  @JsonProperty
  public String getColumnName() {
    return columnName;
  }

  @JsonProperty
  public ColumnHandle getInternalColumnHandler() {
    return internalColumnHandler;
  }

  @Override
  public int hashCode() {
    return columnName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    GravitonColumnHandle other = (GravitonColumnHandle) obj;
    return columnName.equals(other.columnName);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("columnName", columnName).toString();
  }
}
