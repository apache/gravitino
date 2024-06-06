/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.trino.spi.connector.ColumnHandle;
import java.util.Objects;

/**
 * The GravitinoTableHandle is used to transform column information between Trino and Gravitino, as
 * well as to wrap the inner connector column handle for data access.
 */
public final class GravitinoColumnHandle implements ColumnHandle, GravitinoHandle<ColumnHandle> {

  private final String columnName;
  private HandleWrapper<ColumnHandle> handleWrapper = new HandleWrapper<>(ColumnHandle.class);

  @JsonCreator
  public GravitinoColumnHandle(
      @JsonProperty("columnName") String columnName,
      @JsonProperty(HANDLE_STRING) String handleString) {
    Preconditions.checkArgument(columnName != null, "schemaName is not null");
    Preconditions.checkArgument(handleString != null, "internalHandle is not null");

    this.columnName = columnName;
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  public GravitinoColumnHandle(String columnName, ColumnHandle internalColumnHandle) {
    this.columnName = columnName;
    this.handleWrapper = new HandleWrapper<>(internalColumnHandle);
  }

  @JsonProperty
  public String getColumnName() {
    return columnName;
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ColumnHandle getInternalHandle() {
    return handleWrapper.getHandle();
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName);
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
    return Objects.equals(this.columnName, other.columnName);
  }

  @Override
  public String toString() {
    return columnName + "->" + getInternalHandle().toString();
  }
}
