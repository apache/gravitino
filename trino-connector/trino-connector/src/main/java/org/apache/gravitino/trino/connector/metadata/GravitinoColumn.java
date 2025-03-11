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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;

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
