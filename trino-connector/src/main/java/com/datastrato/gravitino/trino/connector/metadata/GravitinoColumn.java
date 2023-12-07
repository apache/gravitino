/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.types.Type;
import java.util.Map;

/** Help Gravitino connector access ColumnMetadata from gravitino client. */
public final class GravitinoColumn {
  private final String name;
  private final Type dataType;
  private final int index;
  private final String comment;
  private final boolean nullable;

  public GravitinoColumn(Column column, int columnIndex) {
    this(column.name(), column.dataType(), columnIndex, column.comment(), column.nullable());
  }

  public GravitinoColumn(String name, Type dataType, int index, String comment, boolean nullable) {
    this.name = name;
    this.dataType = dataType;
    this.index = index;
    this.comment = comment;
    this.nullable = nullable;
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

  public boolean isNullable() {
    return nullable;
  }

  public boolean isHidden() {
    return false;
  }
}
