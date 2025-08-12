/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.trino.connector.metadata;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;

/** Help Gravitino connector access ColumnMetadata from gravitino client. */
public final class GravitinoColumn {
  private final String name;
  private final Type dataType;
  private final int index;
  private final String comment;
  private final boolean nullable;
  private final boolean autoIncrement;
  private final Expression defaultValue;

  // Column properties
  private final Map<String, Object> properties;

  /**
   * Constructs a new GravitinoColumn with the specified column and column index.
   *
   * @param column the column
   * @param columnIndex the column index
   */
  public GravitinoColumn(Column column, int columnIndex) {
    this(
        column.name(),
        column.dataType(),
        columnIndex,
        column.comment(),
        column.nullable(),
        column.autoIncrement(),
        column.defaultValue(),
        ImmutableMap.of());
  }

  /**
   * Constructs a new GravitinoColumn with the specified name, data type, index, comment, nullable,
   * autoIncrement, defaultValue and properties.
   *
   * @param name the name of the column
   * @param dataType the data type of the column
   * @param index the index of the column
   * @param comment the comment of the column
   * @param nullable whether the column is nullable
   * @param autoIncrement whether the column is auto increment
   * @param defaultValue the default value of the column
   * @param properties the properties of the column
   */
  public GravitinoColumn(
      String name,
      Type dataType,
      int index,
      String comment,
      boolean nullable,
      boolean autoIncrement,
      Expression defaultValue,
      Map<String, Object> properties) {
    this.name = name;
    this.dataType = dataType;
    this.index = index;
    this.comment = comment;
    this.nullable = nullable;
    this.autoIncrement = autoIncrement;
    this.defaultValue = defaultValue;
    this.properties = properties;
  }

  /**
   * Constructs a new GravitinoColumn with the specified name, data type, index, comment, nullable,
   * autoIncrement, and properties.
   *
   * @param name the name of the column
   * @param dataType the data type of the column
   * @param index the index of the column
   * @param comment the comment of the column
   * @param nullable whether the column is nullable
   * @param autoIncrement whether the column is auto increment
   * @param properties the properties of the column
   */
  public GravitinoColumn(
      String name,
      Type dataType,
      int index,
      String comment,
      boolean nullable,
      boolean autoIncrement,
      Map<String, Object> properties) {
    this(
        name, dataType, index, comment, nullable, autoIncrement, DEFAULT_VALUE_NOT_SET, properties);
  }

  /**
   * Constructs a new GravitinoColumn with the specified name, data type, index, comment, and
   * nullable.
   *
   * @param name the name of the column
   * @param dataType the data type of the column
   * @param index the index of the column
   * @param comment the comment of the column
   * @param nullable whether the column is nullable
   */
  public GravitinoColumn(String name, Type dataType, int index, String comment, boolean nullable) {
    this(name, dataType, index, comment, nullable, false, ImmutableMap.of());
  }

  /**
   * Retrieves the index of the column.
   *
   * @return the index of the column
   */
  public int getIndex() {
    return index;
  }

  /**
   * Retrieves the properties of the column.
   *
   * @return the properties of the column
   */
  public Map<String, Object> getProperties() {
    return properties;
  }

  /**
   * Retrieves the name of the column.
   *
   * @return the name of the column
   */
  public String getName() {
    return name;
  }

  /**
   * Retrieves the data type of the column.
   *
   * @return the data type of the column
   */
  public Type getType() {
    return dataType;
  }

  /**
   * Retrieves the comment of the column.
   *
   * @return the comment of the column
   */
  public String getComment() {
    return comment;
  }

  /**
   * Retrieves whether the column is nullable.
   *
   * @return whether the column is nullable
   */
  public boolean isNullable() {
    return nullable;
  }

  /**
   * Retrieves whether the column is hidden.
   *
   * @return whether the column is hidden
   */
  public boolean isHidden() {
    return false;
  }

  /**
   * Retrieves whether the column is auto increment.
   *
   * @return whether the column is auto increment
   */
  public boolean isAutoIncrement() {
    return autoIncrement;
  }

  /**
   * Retrieves the default value of the column.
   *
   * @return the default value of the column
   */
  public Expression getDefaultValue() {
    return defaultValue;
  }
}
