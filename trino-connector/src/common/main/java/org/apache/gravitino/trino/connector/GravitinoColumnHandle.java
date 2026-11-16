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
package org.apache.gravitino.trino.connector;

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

  /**
   * Constructs a new GravitinoColumnHandle with the specified column name and handle string.
   *
   * @param columnName the name of the column
   * @param handleString the handle string
   */
  @JsonCreator
  public GravitinoColumnHandle(
      @JsonProperty("columnName") String columnName,
      @JsonProperty(HANDLE_STRING) String handleString) {
    Preconditions.checkArgument(columnName != null, "schemaName is not null");
    Preconditions.checkArgument(handleString != null, "internalHandle is not null");

    this.columnName = columnName;
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  /**
   * Constructs a new GravitinoColumnHandle with the specified column name and internal column
   * handle.
   *
   * @param columnName the name of the column
   * @param internalColumnHandle the internal column handle
   */
  public GravitinoColumnHandle(String columnName, ColumnHandle internalColumnHandle) {
    this.columnName = columnName;
    this.handleWrapper = new HandleWrapper<>(internalColumnHandle);
  }

  /**
   * Gets the name of the column.
   *
   * @return the column name
   */
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
