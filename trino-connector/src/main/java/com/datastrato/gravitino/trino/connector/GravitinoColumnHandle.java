/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.trino.spi.connector.ColumnHandle;

/**
 * The GravitinoColumnHandle is used to transform column information between Trino and Gravitino, as
 * well as to wrap the inner connector column handle for data access.
 */
public final class GravitinoColumnHandle implements ColumnHandle {
  private final String columnName;
  private final ColumnHandle internalColumnHandler;

  @JsonCreator
  public GravitinoColumnHandle(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("internalColumnHandler") ColumnHandle columnHandle) {
    Preconditions.checkArgument(columnName != null, "columnName is not null");
    Preconditions.checkArgument(columnHandle != null, "columnHandle is not null");
    this.columnName = columnName;
    this.internalColumnHandler = columnHandle;
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

    GravitinoColumnHandle other = (GravitinoColumnHandle) obj;
    return columnName.equals(other.columnName);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("columnName", columnName).toString();
  }
}
