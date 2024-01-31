/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.types.Type;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Help Gravitino connector access ColumnMetadata from gravitino client. */
public final class GravitinoColumn {
  private final String name;
  private final Type dataType;
  private final int index;
  private final String comment;
  private final boolean nullable;
  private final boolean autoIncrement;

  // Column properties
  private final Map<String, Object> properties;

  public GravitinoColumn(Column column, int columnIndex) {
    this(
        column.name(),
        column.dataType(),
        columnIndex,
        column.comment(),
        column.nullable(),
        column.autoIncrement(),
        ImmutableMap.of());
  }

  public GravitinoColumn(
      String name,
      Type dataType,
      int index,
      String comment,
      boolean nullable,
      boolean autoIncrement,
      Map<String, Object> properties) {
    this.name = name;
    this.dataType = dataType;
    this.index = index;
    this.comment = comment;
    this.nullable = nullable;
    this.autoIncrement = autoIncrement;
    this.properties = properties;
  }

  public GravitinoColumn(String name, Type dataType, int index, String comment, boolean nullable) {
    this(name, dataType, index, comment, nullable, false, ImmutableMap.of());
  }

  public int getIndex() {
    return index;
  }

  public Map<String, Object> getProperties() {
    return properties;
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

  public boolean isAutoIncrement() {
    return autoIncrement;
  }
}
