/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import static java.util.Objects.requireNonNull;

import com.datastrato.graviton.rel.Column;
import io.substrait.type.Type;
import java.util.Map;

/** Help Graviton connector access ColumnMetadata from graviton client. */
public final class GravitonColumn {
  private final String name;
  private final Type dataType;
  private final int index;
  private final String comment;

  public GravitonColumn(Column column, int columnIndex) {
    this.name = column.name();
    this.dataType = column.dataType();
    this.index = columnIndex;
    this.comment = column.comment();
    requireNonNull(column, "column is null or is empty");
  }

  public GravitonColumn(String name, Type dataType, int index, String comment) {
    this.name = name;
    this.dataType = dataType;
    this.index = index;
    this.comment = comment;
  }

  public int getIndex() {
    return index;
  }

  public Map<String, String> getProperties() {
    return null;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return dataType;
  }

  public String getComment() {
    return comment;
  }
}
