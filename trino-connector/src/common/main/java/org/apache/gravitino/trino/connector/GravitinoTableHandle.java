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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import java.util.Objects;

/**
 * The GravitinoTableHandle is used to transform table information between Trino and Gravitino, as
 * well as to wrap the inner connector table handle for data access.
 */
public final class GravitinoTableHandle
    implements ConnectorTableHandle, GravitinoHandle<ConnectorTableHandle> {

  private final String schemaName;
  private final String tableName;

  private HandleWrapper<ConnectorTableHandle> handleWrapper =
      new HandleWrapper<>(ConnectorTableHandle.class);

  /**
   * Constructs a new GravitinoTableHandle with the specified schema name, table name, and handle
   * string.
   *
   * @param schemaName the name of the schema
   * @param tableName the name of the table
   * @param handleString the handle string representation
   * @throws IllegalArgumentException if any parameter is null
   */
  @JsonCreator
  public GravitinoTableHandle(
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty(HANDLE_STRING) String handleString) {
    Preconditions.checkArgument(schemaName != null, "schemaName is not null");
    Preconditions.checkArgument(tableName != null, "tableName is not null");
    Preconditions.checkArgument(handleString != null, "handleString is not null");

    this.schemaName = schemaName;
    this.tableName = tableName;
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  /**
   * Constructs a new GravitinoTableHandle with the specified schema name, table name, and internal
   * table handle.
   *
   * @param schemaName the name of the schema
   * @param tableName the name of the table
   * @param internalTableHandle the internal table handle
   * @throws IllegalArgumentException if any parameter is null
   */
  public GravitinoTableHandle(
      String schemaName, String tableName, ConnectorTableHandle internalTableHandle) {
    Preconditions.checkArgument(schemaName != null, "schemaName is not null");
    Preconditions.checkArgument(tableName != null, "tableName is not null");
    Preconditions.checkArgument(internalTableHandle != null, "internalTableHandle is not null");

    this.schemaName = schemaName;
    this.tableName = tableName;
    this.handleWrapper = new HandleWrapper<>(internalTableHandle);
  }

  /**
   * Gets the name of the schema.
   *
   * @return the schema name
   */
  @JsonProperty
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * Gets the name of the table.
   *
   * @return the table name
   */
  @JsonProperty
  public String getTableName() {
    return tableName;
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ConnectorTableHandle getInternalHandle() {
    return handleWrapper.getHandle();
  }

  /**
   * Converts this table handle to a SchemaTableName object.
   *
   * @return a SchemaTableName representing this table's schema and name
   */
  public SchemaTableName toSchemaTableName() {
    return new SchemaTableName(schemaName, tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaName, tableName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    GravitinoTableHandle other = (GravitinoTableHandle) obj;
    return Objects.equals(this.schemaName, other.schemaName)
        && Objects.equals(this.tableName, other.tableName);
  }

  @Override
  public String toString() {
    return String.format("%s.%s->%s", schemaName, tableName, getInternalHandle().toString());
  }
}
