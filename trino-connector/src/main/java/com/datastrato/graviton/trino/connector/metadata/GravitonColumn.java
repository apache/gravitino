/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import com.datastrato.graviton.rel.Column;
import com.google.common.base.Preconditions;
import io.substrait.type.Type;
import java.util.Map;

/** Help Graviton connector access ColumnMetadata from graviton client. */
public final class GravitonColumn {
  private final Column column;
  private final int index;

  public GravitonColumn(Column column, int columnIndex) {
    this.column = column;
    this.index = columnIndex;
    Preconditions.checkArgument(column != null, "column is not null");
  }

  public int getIndex() {
    return index;
  }

  public Map<String, String> getProperties() {
    return Map.of();
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
