/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import static java.util.Objects.requireNonNull;

import com.datastrato.graviton.rel.Column;
import io.substrait.type.Type;
import java.util.Map;

public final class GravitonColumn {
  private final Column column;
  private final int index;

  public GravitonColumn(Column column, int columnIndex) {
    this.column = column;
    this.index = columnIndex;
    requireNonNull(column, "column is null or is empty");
  }

  public int getIndex() {
    return index;
  }

  public Map<String, String> getProperties() {
    return null;
  }

  public String getName() {
    return column.name();
  }

  public Type getType() {
    return column.dataType();
  }

  public String getComment() {
    return column.comment();
  }
}
